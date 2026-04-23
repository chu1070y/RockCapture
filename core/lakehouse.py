"""
Core pipeline orchestration for the LakeHouse workflow.
"""

import threading
import time
from typing import cast

from connectors import (
    BaseDBConnector,
    MetadataWriter,
    MinIOConnector,
    MySQLConnector,
    PostgreSQLConnector,
    SparkSessionManager,
)
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
from core.logger import get_logger, setup_logging
from core.pipeline_runner import (
    build_flat_tables,
    read_jdbc_where,
    run_compaction,
    run_parallel_load,
)

log = get_logger(__name__)


class LakehousePipeline:
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
        single_shot: bool = False,
        spark_temp_dir: str | None = None,
    ) -> None:
        db_type = db_type.lower()
        if db_type not in self.SUPPORTED_DB_TYPES:
            raise ValueError(
                f"Unsupported DB type '{db_type}'. "
                f"Use one of {sorted(self.SUPPORTED_DB_TYPES)}"
            )

        self._db_type = db_type
        self._single_shot = single_shot
        self._spark_temp_dir = spark_temp_dir

        logging_cfg = LoggingConfig.from_yaml(config_file)
        self._minio_cfg = MinIOConfig.from_yaml(config_file)
        self._spark_cfg = SparkConfig.from_yaml(config_file)
        self._iceberg_cfg = IcebergConfig.from_yaml(config_file)
        self._pipeline_cfg = PipelineConfig.from_yaml(config_file)

        self._db_cfg: BaseDBConfig = self._build_db_config(
            db_type,
            str(host),
            str(port),
            user,
            password,
            pg_database,
            mysql_jdbc_jar,
            pg_jdbc_jar,
        )

        setup_logging(level=logging_cfg.level)

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
            return MySQLConnector(db_cfg)  # type: ignore[arg-type]
        return PostgreSQLConnector(db_cfg)  # type: ignore[arg-type]

    def run(self, cancel_event: threading.Event | None = None) -> str:
        _start = time.monotonic()
        log.info(
            "===== 전체 테이블 스냅샷 시작  (DB 타입: %s) =====",
            self._db_type,
        )
        log.info("===== LakeHouse 파이프라인 시작 =====")
        log.info(
            "DB 타입: %s  |  host=%s:%s  |  user=%s",
            self._db_type,
            self._db_cfg.host,
            self._db_cfg.port,
            self._db_cfg.user,
        )

        metadata_writer = MetadataWriter()

        with SparkSessionManager(
            self._spark_cfg,
            self._db_cfg,
            self._minio_cfg,
            self._iceberg_cfg,
            temp_dir=self._spark_temp_dir,
        ) as spark:
            minio = MinIOConnector(self._minio_cfg, self._iceberg_cfg)

            with self._make_connector(self._db_type, self._db_cfg) as db:
                log.info("[1/3] 데이터베이스 및 테이블 목록 조회")
                databases = db.list_databases()
                db_tables = {db_name: db.list_tables(db_name) for db_name in databases}
                total_tables = sum(len(tables) for tables in db_tables.values())
                log.info("적재 대상 DB %d개, 테이블 %d개", len(db_tables), total_tables)

                if self._pipeline_cfg.stats_max_age_seconds > 0:
                    log.info("[1.5/3] 오래된 통계 정보 선택 갱신")
                    db.refresh_stale_table_stats(
                        db_tables,
                        max_age_seconds=self._pipeline_cfg.stats_max_age_seconds,
                    )

                log.info("[2/3] 스냅숏 캡처")
                db.lock_tables_for_snapshot()
                snapshot_meta = db.init_snapshot(db_tables)
                db.open_worker_connections(workers=self._pipeline_cfg.num_threads)
                db.unlock_tables()

                log.info("[2.5] 스냅숏 메타데이터 MinIO 저장")
                metadata_writer.save_replication_status_to_minio(snapshot_meta, self._minio_cfg)

                log.info("[3/3] 건수 및 PK 정보 수집")
                try:
                    stats = db.collect_all_table_stats(db_tables)
                    if len(stats) == 3:
                        row_counts, pk_info, ctid_pages_map = stats
                    else:
                        row_counts, pk_info = stats
                        ctid_pages_map = {}
                finally:
                    db.close_worker_connections()

            log.info("[4] Spark JDBC 병렬 적재 시작")
            pg_database = (
                cast(PostgreSQLConfig, self._db_cfg).database
                if self._db_type == "postgresql"
                else None
            )
            tasks = build_flat_tables(
                db_tables,
                row_counts,
                pk_info,
                snapshot_meta,
                self._pipeline_cfg,
                pg_database=pg_database,
                ctid_pages_map=ctid_pages_map,
                single_shot=self._single_shot,
            )
            run_parallel_load(
                tasks,
                spark,
                self._db_cfg,
                minio,
                self._pipeline_cfg,
                cancel_event=cancel_event,
            )

            if not (cancel_event and cancel_event.is_set()):
                log.info("[5] Iceberg compaction 시작")
                run_compaction(tasks, spark, minio, cancel_event=cancel_event)

        elapsed = int(time.monotonic() - _start)
        h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
        elapsed_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
        log.info("===== LakeHouse 파이프라인 종료  (총 소요시간: %s) =====", elapsed_str)
        return elapsed_str

    def run_filtered_snapshot(
        self,
        source_db: str,
        source_table: str,
        filter_group: dict,
        cancel_event: threading.Event | None = None,
    ) -> str:
        _start = time.monotonic()
        log.info(
            "===== 조건부 테이블 스냅샷 시작  (DB 타입: %s, source=%s.%s) =====",
            self._db_type,
            source_db,
            source_table,
        )

        with SparkSessionManager(
            self._spark_cfg,
            self._db_cfg,
            self._minio_cfg,
            self._iceberg_cfg,
            temp_dir=self._spark_temp_dir,
        ) as spark:
            if cancel_event and cancel_event.is_set():
                raise RuntimeError("Filtered snapshot was cancelled before Spark load started.")

            df = read_jdbc_where(
                spark,
                self._db_cfg,
                source_db,
                source_table,
                filter_group,
            )

            minio = MinIOConnector(self._minio_cfg, self._iceberg_cfg)
            namespace = (
                ["postgresql", cast(PostgreSQLConfig, self._db_cfg).database, source_db]
                if self._db_type == "postgresql"
                else ["mysql", source_db]
            )
            minio.write_iceberg(df, namespace, source_table)

            if not (cancel_event and cancel_event.is_set()):
                log.info("[filtered] Iceberg compaction 시작  (%s.%s)", source_db, source_table)
                minio.compact_iceberg(spark, namespace, source_table)

        elapsed = int(time.monotonic() - _start)
        h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
        elapsed_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
        log.info(
            "===== 조건부 테이블 스냅샷 종료  (DB 타입: %s, source=%s.%s, 총 소요시간: %s) =====",
            self._db_type,
            source_db,
            source_table,
            elapsed_str,
        )
        return elapsed_str
