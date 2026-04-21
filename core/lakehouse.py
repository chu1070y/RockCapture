"""
core/lakehouse.py — LakeHouse 파이프라인 핵심 로직

LakehousePipeline 클래스가 파이프라인 전체 단계를 순서대로 수행한다.
FastAPI 요청 모델(PipelineRequest)을 직접 받아 동작하므로
main.py(API 레이어)와 분리되어 독립적으로 테스트·재사용 가능하다.
"""
import threading
import time

from core.config import (
    BaseDBConfig,
    IcebergConfig,
    LoggingConfig,
    MinIOConfig,
    MySQLConfig,
    PipelineConfig,
    PostgreSQLConfig,
    SparkConfig,
)
from typing import cast
from connectors import (
    BaseDBConnector,
    MetadataWriter,
    MinIOConnector,
    MySQLConnector,
    PostgreSQLConnector,
    SparkSessionManager,
)
from core.logger import get_logger, setup_logging
from core.pipeline_runner import build_flat_tables, run_parallel_load

log = get_logger(__name__)


class LakehousePipeline:
    """
    LakeHouse 전체 파이프라인을 캡슐화한 클래스.

    Parameters
    ----------
    db_type : str
        연결할 DB 종류. "mysql" 또는 "postgresql".
    host : str
        DB 서버 호스트 (IP 또는 도메인).
    port : int | str
        DB 포트 번호.
    user : str
        DB 접속 계정.
    password : str
        DB 접속 비밀번호.
    pg_database : str, optional
        PostgreSQL 전용. 접속할 데이터베이스 이름 (기본값: "postgres").
    mysql_jdbc_jar : str, optional
        MySQL JDBC 드라이버 JAR 경로.
    pg_jdbc_jar : str, optional
        PostgreSQL JDBC 드라이버 JAR 경로.
    config_file : str, optional
        MinIO / Spark / Iceberg / Pipeline 설정 YAML 경로.
    """

    SUPPORTED_DB_TYPES = {"mysql", "postgresql"}

    def __init__(
        self,
        db_type: str,
        host: str,
        port: int | str,
        user: str,
        password: str,
        *,
        pg_database: str = "postgres",
        mysql_jdbc_jar: str = "drivers/mysql-connector-j-9.2.0.jar",
        pg_jdbc_jar: str = "drivers/postgresql-42.7.9.jar",
        config_file: str = "pipeline.yaml",
    ) -> None:
        db_type = db_type.lower()
        if db_type not in self.SUPPORTED_DB_TYPES:
            raise ValueError(
                f"지원하지 않는 DB 타입: '{db_type}'. "
                f"사용 가능: {sorted(self.SUPPORTED_DB_TYPES)}"
            )

        self._db_type = db_type

        logging_cfg        = LoggingConfig.from_yaml(config_file)
        self._minio_cfg    = MinIOConfig.from_yaml(config_file)
        self._spark_cfg    = SparkConfig.from_yaml(config_file)
        self._iceberg_cfg  = IcebergConfig.from_yaml(config_file)
        self._pipeline_cfg = PipelineConfig.from_yaml(config_file)

        self._db_cfg: BaseDBConfig = self._build_db_config(
            db_type, str(host), str(port), user, password,
            pg_database, mysql_jdbc_jar, pg_jdbc_jar,
        )

        setup_logging(level=logging_cfg.level)

    # ── 내부 헬퍼 ─────────────────────────────────────────────────

    @staticmethod
    def _build_db_config(
        db_type: str,
        host: str,
        port: str,
        user: str,
        password: str,
        pg_database: str,
        mysql_jdbc_jar: str,
        pg_jdbc_jar: str,
    ) -> BaseDBConfig:
        if db_type == "mysql":
            return MySQLConfig(
                host=host,
                port=port,
                user=user,
                password=password,
                jdbc_jar_path=mysql_jdbc_jar,
            )
        return PostgreSQLConfig(
            host=host,
            port=port,
            user=user,
            password=password,
            database=pg_database,
            jdbc_jar_path=pg_jdbc_jar,
        )

    @staticmethod
    def _make_connector(db_type: str, db_cfg: BaseDBConfig) -> BaseDBConnector:
        if db_type == "mysql":
            return MySQLConnector(db_cfg)       # type: ignore[arg-type]
        return PostgreSQLConnector(db_cfg)      # type: ignore[arg-type]

    # ── 공개 API ──────────────────────────────────────────────────

    def run(self, cancel_event: threading.Event | None = None) -> str:
        """파이프라인 전체 단계를 순서대로 실행하고 소요시간 문자열을 반환한다."""
        _start = time.monotonic()
        log.info("===== LakeHouse 파이프라인 시작 =====")
        log.info(
            "DB 타입: %s  |  host=%s:%s  |  user=%s",
            self._db_type, self._db_cfg.host, self._db_cfg.port, self._db_cfg.user,
        )

        metadata_writer = MetadataWriter()

        with SparkSessionManager(
            self._spark_cfg, self._db_cfg, self._minio_cfg, self._iceberg_cfg
        ) as spark:
            minio = MinIOConnector(self._minio_cfg, self._iceberg_cfg)

            with self._make_connector(self._db_type, self._db_cfg) as db:

                # 1. 테이블 목록 조회
                log.info("[1/3] 데이터베이스 및 테이블 목록 조회")
                databases = db.list_databases()
                db_tables = {d: db.list_tables(d) for d in databases}
                total_tables = sum(len(t) for t in db_tables.values())
                log.info("적재 대상: DB %d개, 테이블 %d개", len(db_tables), total_tables)

                # 2. 스냅숏 캡처
                #    MySQL     : FTWRL → binlog 위치 기록 → 워커 커넥션 확보 → UNLOCK
                #    PostgreSQL: REPEATABLE READ + pg_export_snapshot() → WAL LSN 기록 → 워커 import → no-op
                log.info("[2/3] 스냅숏 캡처")
                db.lock_tables_for_snapshot()
                snapshot_meta = db.init_snapshot(db_tables)
                db.open_worker_connections(workers=self._pipeline_cfg.num_threads)
                db.unlock_tables()

                # 스냅숏 직후 MinIO에 복제 상태 저장
                log.info("[2.5] 스냅숏 메타데이터 MinIO 저장")
                metadata_writer.save_replication_status_to_minio(snapshot_meta, self._minio_cfg)

                # 3. 행 수 및 PK 정보 수집
                log.info("[3/3] 행 수 및 PK 정보 수집")
                stats = db.collect_all_table_stats(db_tables)
                if len(stats) == 3:
                    row_counts, pk_info, ctid_pages_map = stats
                else:
                    row_counts, pk_info = stats
                    ctid_pages_map = {}
                db.close_worker_connections()

            # 4. Spark JDBC 병렬 적재
            log.info("[4] Spark JDBC 병렬 적재 시작")
            pg_database = (
                cast(PostgreSQLConfig, self._db_cfg).database
                if self._db_type == "postgresql" else None
            )
            tasks = build_flat_tables(
                db_tables, row_counts, pk_info, snapshot_meta, self._pipeline_cfg,
                pg_database=pg_database,
                ctid_pages_map=ctid_pages_map,
            )
            run_parallel_load(tasks, spark, self._db_cfg, minio, self._pipeline_cfg,
                              cancel_event=cancel_event)

        elapsed = int(time.monotonic() - _start)
        h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
        elapsed_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
        log.info("===== LakeHouse 파이프라인 종료  (총 소요시간: %s) =====", elapsed_str)
        return elapsed_str
