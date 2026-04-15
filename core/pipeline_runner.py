"""
pipeline_runner.py

Spark JDBC 기반 병렬 적재 구현.
MySQL 커넥션은 스냅숏 캡처·통계 수집까지만 사용하고 닫은 뒤,
Spark JDBC가 sessionInitStatement로 자체 일관된 스냅숏을 확보하여 읽음.

- TableTask        : 테이블 단위 작업 데이터
- build_flat_tables: 테이블 목록을 TableTask 리스트로 변환
- run_parallel_load: 1차 병렬 적재 + 2차 재시도
"""
from __future__ import annotations

import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from queue import Empty, Queue

from pyspark.sql import SparkSession

from core.config import MinIOConfig, MySQLConfig, PipelineConfig
from connectors import MinIOConnector
from connectors.metadata_writer import SnapshotMetadata, TableResult
from core.logger import get_logger

log = get_logger(__name__)


# ── 데이터 ───────────────────────────────────────────────────

@dataclass
class TableTask:
    db: str
    table: str
    total_rows: int
    pk_column: str | None     # JDBC 파티셔닝용 숫자형 PK (없으면 단일 파티션)
    lower_bound: int          # PK MIN
    upper_bound: int          # PK MAX
    num_partitions: int       # Spark JDBC 병렬 파티션 수
    result: TableResult
    batch_size: int = 0       # 0=전체 읽기, >0=PK 범위 배치 적재


# ── JDBC 유틸 ─────────────────────────────────────────────────

def _jdbc_base_options(mysql_cfg: MySQLConfig) -> dict:
    return {
        "url":      mysql_cfg.jdbc_url,
        "user":     mysql_cfg.user,
        "password": mysql_cfg.password,
        "driver":   mysql_cfg.jdbc_driver,
        # 각 Spark JDBC 커넥션이 열릴 때 일관된 스냅숏 확보
        "sessionInitStatement": "START TRANSACTION WITH CONSISTENT SNAPSHOT",
    }


def _read_jdbc(
    spark: SparkSession,
    mysql_cfg: MySQLConfig,
    db: str,
    table: str,
    num_partitions: int = 1,
    pk_column: str | None = None,
    lower_bound: int = 0,
    upper_bound: int = 0,
    fetch_size: int = 10_000,
):
    fqn = f"`{db}`.`{table}`"
    opts = {
        **_jdbc_base_options(mysql_cfg),
        "dbtable":   fqn,
        "fetchsize": str(fetch_size),
    }
    if pk_column and num_partitions > 1 and upper_bound > lower_bound:
        opts.update({
            "partitionColumn": pk_column,
            "lowerBound":      str(lower_bound),
            "upperBound":      str(upper_bound),
            "numPartitions":   str(num_partitions),
        })
    return spark.read.format("jdbc").options(**opts).load()


def _read_jdbc_range(
    spark: SparkSession,
    mysql_cfg: MySQLConfig,
    db: str,
    table: str,
    pk_column: str,
    lo: int,
    hi: int,
    fetch_size: int = 10_000,
):
    query = (
        f"(SELECT * FROM `{db}`.`{table}`"
        f" WHERE `{pk_column}` BETWEEN {lo} AND {hi}) AS t"
    )
    opts = {
        **_jdbc_base_options(mysql_cfg),
        "dbtable":   query,
        "fetchsize": str(fetch_size),
    }
    return spark.read.format("jdbc").options(**opts).load()


# ── 배치 적재 ─────────────────────────────────────────────────

def _load_in_batches(
    tag: str,
    task: TableTask,
    spark: SparkSession,
    mysql_cfg: MySQLConfig,
    minio: MinIOConnector,
) -> None:
    """
    대형 테이블을 PK 범위 기준 batch_size 단위로 분할하여 순서대로 적재.
    첫 번째 배치는 createOrReplace, 이후 배치는 append.
    """
    lo, hi = task.lower_bound, task.upper_bound
    batch_size = task.batch_size
    total_batches = math.ceil((hi - lo + 1) / batch_size)
    log.info(
        "%s 배치 적재 시작  %s.%s  (rows=%d, batch_size=%d, batches=%d)",
        tag, task.db, task.table, task.total_rows, batch_size, total_batches,
    )

    batch_lo = lo
    batch_num = 0
    while batch_lo <= hi:
        batch_hi = min(batch_lo + batch_size - 1, hi)
        batch_num += 1
        log.info(
            "%s 배치 %d/%d  pk %d ~ %d",
            tag, batch_num, total_batches, batch_lo, batch_hi,
        )
        df = _read_jdbc_range(spark, mysql_cfg, task.db, task.table,
                              task.pk_column, batch_lo, batch_hi)
        if batch_num == 1:
            minio.write_iceberg(df, task.db, task.table)
        else:
            minio.append_iceberg(df, task.db, task.table)
        batch_lo = batch_hi + 1

    log.info("%s 배치 적재 완료  %s.%s  (%d배치)", tag, task.db, task.table, batch_num)


# ── 워커 ─────────────────────────────────────────────────────

def _worker(
    tag: str,
    task_queue: Queue,
    failed_list: list[TableTask],
    failed_lock: threading.Lock,
    stop_event: threading.Event,
    spark: SparkSession,
    mysql_cfg: MySQLConfig,
    minio: MinIOConnector,
) -> None:
    while not stop_event.is_set():
        try:
            task = task_queue.get(timeout=0.5)
        except Empty:
            break

        try:
            log.info(
                "%s 시작  %s.%s  (rows=%d, partitions=%d, pk=%s, batch_size=%s)",
                tag, task.db, task.table,
                task.total_rows, task.num_partitions, task.pk_column,
                task.batch_size if task.batch_size > 0 else "없음",
            )
            if task.batch_size > 0 and task.pk_column:
                _load_in_batches(tag, task, spark, mysql_cfg, minio)
            else:
                df = _read_jdbc(
                    spark, mysql_cfg,
                    task.db, task.table,
                    num_partitions=task.num_partitions,
                    pk_column=task.pk_column,
                    lower_bound=task.lower_bound,
                    upper_bound=task.upper_bound,
                )
                minio.write_iceberg(df, task.db, task.table)
            task.result.row_count = task.total_rows
            log.info("%s 완료  %s.%s", tag, task.db, task.table)

        except Exception as e:
            log.error(
                "%s 실패  %s.%s — %s",
                tag, task.db, task.table, e, exc_info=True,
            )
            with failed_lock:
                failed_list.append(task)
        finally:
            task_queue.task_done()


def _run_workers(
    tasks: list[TableTask],
    num_workers: int,
    stop_event: threading.Event,
    spark: SparkSession,
    mysql_cfg: MySQLConfig,
    minio: MinIOConnector,
    tag_prefix: str = "W",
) -> list[TableTask]:
    failed_list: list[TableTask] = []
    failed_lock = threading.Lock()
    task_queue: Queue = Queue()
    for t in tasks:
        task_queue.put(t)

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = [
            pool.submit(
                _worker,
                f"[{tag_prefix}{i}]", task_queue,
                failed_list, failed_lock, stop_event,
                spark, mysql_cfg, minio,
            )
            for i in range(num_workers)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                log.exception("[%s] 워커 예외", tag_prefix)

    return failed_list


# ── 공개 API ──────────────────────────────────────────────────

def build_flat_tables(
    db_tables: dict[str, list[str]],
    row_counts: dict[tuple[str, str], int],
    pk_info: dict[tuple[str, str], tuple[str | None, int, int]],
    snapshot_meta: SnapshotMetadata,
    cfg: PipelineConfig,
) -> list[TableTask]:
    """
    테이블 목록을 TableTask 리스트로 변환.

    pk_info: {(db, table): (pk_column, lower_bound, upper_bound)}
    num_partitions = min(ceil(total_rows / chunk_size), num_threads)
    대형 테이블(total_rows >= large_table_threshold)이고 PK가 있으면 batch_size 설정.
    """
    result_map = {(r.database, r.name): r for r in snapshot_meta.tables}
    tasks = []
    for db, tables in db_tables.items():
        for table in tables:
            total_rows = row_counts[(db, table)]
            pk_col, lo, hi = pk_info.get((db, table), (None, 0, 0))

            if pk_col and hi > lo:
                num_partitions = min(
                    math.ceil(total_rows / cfg.chunk_size),
                    cfg.num_threads,
                )
                num_partitions = max(num_partitions, 1)
            else:
                num_partitions = 1

            is_large = total_rows >= cfg.large_table_threshold
            batch_size = cfg.large_table_batch_size if (is_large and pk_col and hi > lo) else 0

            tasks.append(TableTask(
                db=db, table=table,
                total_rows=total_rows,
                pk_column=pk_col,
                lower_bound=lo,
                upper_bound=hi,
                num_partitions=num_partitions,
                result=result_map[(db, table)],
                batch_size=batch_size,
            ))
    return tasks


def run_parallel_load(
    tasks: list[TableTask],
    spark: SparkSession,
    mysql_cfg: MySQLConfig,
    minio: MinIOConnector,
    cfg: PipelineConfig,
) -> None:
    """
    Spark JDBC 기반 병렬 적재 실행.

    소형 테이블: num_threads 워커가 병렬 처리.
    대형 테이블: large_table_workers 워커로 배치 직렬 처리 (OOM 방지).
    2차: 실패 테이블 재시도. 재시도도 실패 시 SystemExit(1).
    """
    stop_event = threading.Event()

    small_tasks = [t for t in tasks if t.total_rows < cfg.large_table_threshold]
    large_tasks = [t for t in tasks if t.total_rows >= cfg.large_table_threshold]

    log.info(
        "적재 분류  (소형=%d개 병렬, 대형=%d개 배치, 임계값=%d rows)",
        len(small_tasks), len(large_tasks), cfg.large_table_threshold,
    )
    if large_tasks:
        log.info(
            "대형 테이블 목록: %s",
            [f"{t.db}.{t.table}({t.total_rows:,})" for t in large_tasks],
        )

    failed: list[TableTask] = []

    if small_tasks:
        num_workers = min(cfg.num_threads, len(small_tasks))
        log.info("1차 소형 적재  (workers=%d, tables=%d)", num_workers, len(small_tasks))
        failed += _run_workers(small_tasks, num_workers, stop_event, spark, mysql_cfg, minio)

    if large_tasks:
        num_large_workers = min(cfg.large_table_workers, len(large_tasks))
        log.info("1차 대형 적재  (workers=%d, tables=%d)", num_large_workers, len(large_tasks))
        failed += _run_workers(large_tasks, num_large_workers, stop_event, spark, mysql_cfg, minio, tag_prefix="LARGE")

    if not failed:
        return

    log.warning(
        "재시도 대상 %d개: %s",
        len(failed), [f"{t.db}.{t.table}" for t in failed],
    )

    retry_small = [t for t in failed if t.total_rows < cfg.large_table_threshold]
    retry_large = [t for t in failed if t.total_rows >= cfg.large_table_threshold]
    second_failed: list[TableTask] = []

    if retry_small:
        num_workers = min(cfg.num_threads, len(retry_small))
        log.info("2차 소형 재시도  (workers=%d, tables=%d)", num_workers, len(retry_small))
        second_failed += _run_workers(retry_small, num_workers, stop_event, spark, mysql_cfg, minio, tag_prefix="RETRY")

    if retry_large:
        num_large_workers = min(cfg.large_table_workers, len(retry_large))
        log.info("2차 대형 재시도  (workers=%d, tables=%d)", num_large_workers, len(retry_large))
        second_failed += _run_workers(retry_large, num_large_workers, stop_event, spark, mysql_cfg, minio, tag_prefix="RETRY_LARGE")

    if second_failed:
        bad = [f"{t.db}.{t.table}" for t in second_failed]
        log.critical("재시도 실패로 전체 종료  문제 테이블: %s", bad)
        stop_event.set()
        raise SystemExit(1)
