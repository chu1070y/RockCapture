import time

from core.config import IcebergConfig, LoggingConfig, MinIOConfig, MySQLConfig, PipelineConfig, SparkConfig
from connectors import MetadataWriter, MinIOConnector, MySQLConnector, SparkSessionManager
from core.logger import get_logger, setup_logging
from core.pipeline_runner import build_flat_tables, run_parallel_load

log = get_logger(__name__)

_CFG = "pipeline.yaml"
logging_cfg  = LoggingConfig.from_yaml(_CFG)
mysql_cfg    = MySQLConfig.from_yaml(_CFG)
minio_cfg    = MinIOConfig.from_yaml(_CFG)
spark_cfg    = SparkConfig.from_yaml(_CFG)
iceberg_cfg  = IcebergConfig.from_yaml(_CFG)
pipeline_cfg = PipelineConfig.from_yaml(_CFG)

setup_logging(level=logging_cfg.level)


if __name__ == "__main__":
    _start = time.monotonic()
    log.info("===== LakeHouse 파이프라인 시작 =====")

    metadata_writer = MetadataWriter(output_dir="metadata")

    with SparkSessionManager(spark_cfg, mysql_cfg, minio_cfg, iceberg_cfg) as spark:
        minio = MinIOConnector(minio_cfg, iceberg_cfg)

        with MySQLConnector(mysql_cfg) as mysql:

            # 1. 테이블 목록 조회
            log.info("[1/3] 데이터베이스 및 테이블 목록 조회")
            databases = mysql.list_databases()
            db_tables = {db: mysql.list_tables(db) for db in databases}
            total_tables = sum(len(t) for t in db_tables.values())
            log.info("적재 대상: DB %d개, 테이블 %d개", len(db_tables), total_tables)

            # 2. 스냅숏 캡처 (FTWRL → binlog 위치 기록 → 워커 커넥션 확보 → UNLOCK)
            log.info("[2/3] 스냅숏 캡처")
            mysql.lock_tables_for_snapshot()
            snapshot_meta = mysql.init_snapshot(db_tables)
            mysql.open_worker_connections(workers=pipeline_cfg.num_threads)
            mysql.unlock_tables()

            # 3. 행 수 및 PK 정보 수집
            log.info("[3/3] 행 수 및 PK 정보 수집")
            row_counts, pk_info = mysql.collect_all_table_stats(db_tables)
            mysql.close_worker_connections()

        # 4. Spark JDBC 병렬 적재
        # sessionInitStatement로 각 JDBC 커넥션이 자체 일관된 스냅숏 확보
        log.info("[4] Spark JDBC 병렬 적재 시작")
        tasks = build_flat_tables(db_tables, row_counts, pk_info, snapshot_meta, pipeline_cfg)
        run_parallel_load(tasks, spark, mysql_cfg, minio, pipeline_cfg)

        # 5. 메타데이터 저장
        log.info("[5] 메타데이터 저장")
        metadata_writer.save_local(snapshot_meta)
        metadata_writer.save_to_minio(
            snapshot_meta, spark,
            f"s3a://{minio_cfg.bucket}/_metadata/snapshot",
        )
        metadata_writer.save_binlog_status_to_minio(snapshot_meta, minio_cfg)

    elapsed = int(time.monotonic() - _start)
    h, m, s = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60
    elapsed_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
    log.info("===== LakeHouse 파이프라인 종료  (총 소요시간: %s) =====", elapsed_str)
