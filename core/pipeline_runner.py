"""
pipeline_runner.py

Spark JDBC 기반 병렬 적재 구현.
DB 커넥션은 스냅숏 캡처·통계 수집까지만 사용하고 닫은 뒤,
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

from core.config import BaseDBConfig, PipelineConfig
from connectors import MinIOConnector
from connectors.metadata_writer import SnapshotMetadata, TableResult
from core.logger import get_logger

log = get_logger(__name__)


# ── 데이터 ───────────────────────────────────────────────────

@dataclass
class TableTask:
    db: str                   # MySQL: 데이터베이스명 / PostgreSQL: 스키마명 (JDBC fqn용)
    table: str
    total_rows: int
    pk_column: str | None     # JDBC 파티셔닝용 숫자형 PK (없으면 단일 파티션)
    lower_bound: int          # PK MIN
    upper_bound: int          # PK MAX
    num_partitions: int       # Spark JDBC 병렬 파티션 수
    result: TableResult
    batch_size: int = 0       # 0=전체 읽기, >0=PK 범위 배치 적재
    pg_database: str | None = None  # PostgreSQL 전용: 실제 데이터베이스명 (Iceberg 최상위 namespace)


def _iceberg_namespace(task: "TableTask") -> list[str]:
    """
    Iceberg 저장 경로의 namespace 목록 반환.

    MySQL      : [db]              → catalog.db.table
    PostgreSQL : [pg_database, db] → catalog.pg_database.schema.table
    """
    if task.pg_database:
        return [task.pg_database, task.db]
    return [task.db]


# ── JDBC 유틸 ─────────────────────────────────────────────────

def _jdbc_base_options(db_cfg: BaseDBConfig) -> dict:
    opts = {
        "url":      db_cfg.jdbc_url,
        "user":     db_cfg.user,
        "password": db_cfg.password,
        "driver":   db_cfg.jdbc_driver,
    }
    # DB 종류에 따라 세션 초기화 SQL이 다름
    # MySQL: START TRANSACTION WITH CONSISTENT SNAPSHOT
    # PostgreSQL: "" (빈 문자열 → 옵션 미포함)
    if db_cfg.session_init_statement:
        opts["sessionInitStatement"] = db_cfg.session_init_statement
    return opts


def _read_jdbc(
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    db: str,
    table: str,
    num_partitions: int = 1,
    pk_column: str | None = None,
    lower_bound: int = 0,
    upper_bound: int = 0,
    fetch_size: int = 10_000,
):
    fqn = db_cfg.fqn(db, table)
    opts = {
        **_jdbc_base_options(db_cfg),
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
    db_cfg: BaseDBConfig,
    db: str,
    table: str,
    pk_column: str,
    lo: int,
    hi: int,
    fetch_size: int = 10_000,
):
    # DB 방언에 맞는 따옴표 적용 (MySQL: 백틱, PostgreSQL: 큰따옴표)
    col_ref = db_cfg.column_ref(pk_column)
    query = (
        f"(SELECT * FROM {db_cfg.fqn(db, table)}"
        f" WHERE {col_ref} BETWEEN {lo} AND {hi}) AS t"
    )
    opts = {
        **_jdbc_base_options(db_cfg),
        "dbtable":   query,
        "fetchsize": str(fetch_size),
    }
    return spark.read.format("jdbc").options(**opts).load()


# ── 배치 적재 ─────────────────────────────────────────────────

def _load_in_batches(
    tag: str,
    task: TableTask,
    spark: SparkSession,
    db_cfg: BaseDBConfig,
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
        df = _read_jdbc_range(spark, db_cfg, task.db, task.table,
                              task.pk_column, batch_lo, batch_hi)
        ns = _iceberg_namespace(task)
        if batch_num == 1:
            minio.write_iceberg(df, ns, task.table)
        else:
            minio.append_iceberg(df, ns, task.table)
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
    db_cfg: BaseDBConfig,
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
                _load_in_batches(tag, task, spark, db_cfg, minio)
            else:
                df = _read_jdbc(
                    spark, db_cfg,
                    task.db, task.table,
                    num_partitions=task.num_partitions,
                    pk_column=task.pk_column,
                    lower_bound=task.lower_bound,
                    upper_bound=task.upper_bound,
                )
                minio.write_iceberg(df, _iceberg_namespace(task), task.table)
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
    db_cfg: BaseDBConfig,
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
                spark, db_cfg, minio,
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
    pg_database: str | None = None,
) -> list[TableTask]:
    """
    테이블 목록을 TableTask 리스트로 변환.

    pk_info: {(db, table): (pk_column, lower_bound, upper_bound)}
    num_partitions = min(ceil(total_rows / chunk_size), num_threads)
    대형 테이블(total_rows >= large_table_threshold)이고 PK가 있으면 batch_size 설정.

    pg_database: PostgreSQL 전용. 접속 중인 데이터베이스명.
                 설정 시 Iceberg 경로가 catalog.pg_database.schema.table 이 됨.
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
                pg_database=pg_database,
            ))
    return tasks


def _run_phase(
    small_tasks: list[TableTask],
    large_tasks: list[TableTask],
    stop_event: threading.Event,
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    minio: MinIOConnector,
    cfg: PipelineConfig,
    tag_small: str = "W",
    tag_large: str = "LARGE",
) -> list[TableTask]:
    """
    소형·대형 테이블을 동시에 병렬 실행하고 실패 목록을 반환.

    소형: num_threads 워커
    대형: large_table_workers 워커 (각 테이블은 batch 단위로 처리)
    두 그룹은 별도 스레드에서 동시에 실행된다.
    """
    failed: list[TableTask] = []
    failed_lock = threading.Lock()

    def run_small() -> list[TableTask]:
        if not small_tasks:
            return []
        num_workers = min(cfg.num_threads, len(small_tasks))
        log.info("소형 적재 시작  (workers=%d, tables=%d)", num_workers, len(small_tasks))
        return _run_workers(small_tasks, num_workers, stop_event, spark, db_cfg, minio, tag_prefix=tag_small)

    def run_large() -> list[TableTask]:
        if not large_tasks:
            return []
        num_workers = min(cfg.large_table_workers, len(large_tasks))
        log.info("대형 적재 시작  (workers=%d, tables=%d)", num_workers, len(large_tasks))
        return _run_workers(large_tasks, num_workers, stop_event, spark, db_cfg, minio, tag_prefix=tag_large)

    # 소형·대형을 동시에 실행
    with ThreadPoolExecutor(max_workers=2) as phase_pool:
        f_small = phase_pool.submit(run_small)
        f_large = phase_pool.submit(run_large)
        for f in as_completed([f_small, f_large]):
            with failed_lock:
                failed += f.result()

    return failed


def run_parallel_load(
    tasks: list[TableTask],
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    minio: MinIOConnector,
    cfg: PipelineConfig,
) -> None:
    """
    Spark JDBC 기반 병렬 적재 실행.

    소형 테이블 (num_threads 워커) + 대형 테이블 (large_table_workers 워커) 를
    동시에 병렬 실행한다.
    2차: 실패 테이블 재시도. 재시도도 실패 시 SystemExit(1).
    """
    stop_event = threading.Event()

    small_tasks = [t for t in tasks if t.total_rows < cfg.large_table_threshold]
    large_tasks = [t for t in tasks if t.total_rows >= cfg.large_table_threshold]

    log.info(
        "적재 분류  (소형=%d개, 대형=%d개, 임계값=%d rows) — 동시 병렬 실행",
        len(small_tasks), len(large_tasks), cfg.large_table_threshold,
    )
    if large_tasks:
        log.info(
            "대형 테이블 목록: %s",
            [f"{t.db}.{t.table}({t.total_rows:,})" for t in large_tasks],
        )

    # 1차: 소형·대형 동시 실행
    failed = _run_phase(small_tasks, large_tasks, stop_event, spark, db_cfg, minio, cfg)

    if not failed:
        return

    log.warning(
        "재시도 대상 %d개: %s",
        len(failed), [f"{t.db}.{t.table}" for t in failed],
    )

    # 2차: 실패 테이블 재시도 (소형·대형 동시 실행)
    retry_small = [t for t in failed if t.total_rows < cfg.large_table_threshold]
    retry_large = [t for t in failed if t.total_rows >= cfg.large_table_threshold]

    second_failed = _run_phase(
        retry_small, retry_large, stop_event, spark, db_cfg, minio, cfg,
        tag_small="RETRY", tag_large="RETRY_LARGE",
    )

    if second_failed:
        bad = [f"{t.db}.{t.table}" for t in second_failed]
        log.critical("재시도 실패로 전체 종료  문제 테이블: %s", bad)
        stop_event.set()
        raise SystemExit(1)
