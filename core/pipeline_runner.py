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
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from queue import Empty, Queue

from pyspark.sql import SparkSession

from core.config import BaseDBConfig, PipelineConfig
from connectors import MinIOConnector
from connectors.metadata_writer import SnapshotMetadata, TableResult
from core.logger import get_logger

log = get_logger(__name__)

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    batch_size: int = 0              # 0=전체 읽기, >0=배치 적재
    pg_database: str | None = None  # PostgreSQL 전용: 실제 데이터베이스명 (Iceberg 최상위 namespace)
    hash_partition_col: str | None = None  # 텍스트 PK 해시 배치용 컬럼 (pk_column이 None일 때만 설정)
    ctid_batch_pages: int = 0       # PostgreSQL ctid 배치 적재용 배치당 페이지 수 (0=배치 없음)


def _iceberg_namespace(task: "TableTask") -> list[str]:
    """
    Iceberg 저장 경로의 namespace 목록 반환.

    MySQL      : [mysql,      db]              → warehouse/mysql/db/table
    PostgreSQL : [postgresql, pg_database, db] → warehouse/postgresql/pg_database/schema/table
    """
    if task.pg_database:
        return ["postgresql", task.pg_database, task.db]
    return ["mysql", task.db]


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


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, (date, datetime)):
        return f"'{value.isoformat()}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError(f"Unsupported filter value type: {type(value).__name__}")


def _column_ref_from_filter(field: str, db_cfg: BaseDBConfig) -> str:
    if not _SAFE_IDENTIFIER_RE.fullmatch(field):
        raise ValueError(f"Unsupported column name: '{field}'")
    return db_cfg.column_ref(field)


def _condition_to_sql(condition: dict[str, object], db_cfg: BaseDBConfig) -> str:
    field = str(condition["field"])
    op = str(condition["op"])
    value = condition.get("value")
    col_ref = _column_ref_from_filter(field, db_cfg)

    simple_ops = {
        "eq": "=",
        "ne": "!=",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
        "like": "LIKE",
    }

    if op in simple_ops:
        return f"{col_ref} {simple_ops[op]} {_sql_literal(value)}"
    if op == "in":
        if not isinstance(value, list) or not value:
            raise ValueError("'in' requires a non-empty list value.")
        values_sql = ", ".join(_sql_literal(item) for item in value)
        return f"{col_ref} IN ({values_sql})"
    if op == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError("'between' requires exactly two values.")
        return f"{col_ref} BETWEEN {_sql_literal(value[0])} AND {_sql_literal(value[1])}"
    if op == "is_null":
        return f"{col_ref} IS NULL"
    if op == "is_not_null":
        return f"{col_ref} IS NOT NULL"
    raise ValueError(f"Unsupported filter operator: '{op}'")


def _filter_group_to_sql(group: dict[str, object], db_cfg: BaseDBConfig) -> str:
    logic = str(group.get("logic", "and")).upper()
    if logic not in {"AND", "OR"}:
        raise ValueError(f"Unsupported filter logic: '{group.get('logic')}'")

    parts: list[str] = []
    for condition in group.get("conditions", []):
        if not isinstance(condition, dict):
            raise ValueError("Each condition must be an object.")
        parts.append(_condition_to_sql(condition, db_cfg))

    for child_group in group.get("groups", []):
        if not isinstance(child_group, dict):
            raise ValueError("Each child group must be an object.")
        parts.append(_filter_group_to_sql(child_group, db_cfg))

    if not parts:
        raise ValueError("Filter group must contain at least one condition or child group.")

    return "(" + f" {logic} ".join(parts) + ")"


def read_jdbc_where(
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    db: str,
    table: str,
    filter_group: dict[str, object],
    fetch_size: int = 10_000,
):
    where_clause = _filter_group_to_sql(filter_group, db_cfg)
    query = (
        f"(SELECT * FROM {db_cfg.fqn(db, table)} "
        f"WHERE {where_clause}) AS filtered_snapshot"
    )
    opts = {
        **_jdbc_base_options(db_cfg),
        "dbtable": query,
        "fetchsize": str(fetch_size),
    }
    return spark.read.format("jdbc").options(**opts).load()


def _read_jdbc_range(
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    db: str,
    table: str,
    pk_column: str,
    lo: int,
    hi: int,
    num_partitions: int = 1,
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
    if num_partitions > 1 and hi > lo:
        opts.update({
            "partitionColumn": pk_column,
            "lowerBound":      str(lo),
            "upperBound":      str(hi),
            "numPartitions":   str(num_partitions),
        })
    return spark.read.format("jdbc").options(**opts).load()


def _read_jdbc_ctid_parallel(
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    db: str,
    table: str,
    total_pages: int,
    n_partitions: int,
    fetch_size: int = 10_000,
):
    """
    PostgreSQL ctid 기반 병렬 읽기.

    Spark JDBC predicates API를 사용하므로 WHERE 절이 서브쿼리 없이 테이블에
    직접 적용된다. PostgreSQL이 TID Range Scan을 사용할 수 있어 각 파티션이
    해당 페이지 구간만 읽는다.

    predicates = [
        "ctid >= '(0,0)'::tid  AND ctid < '(5000,0)'::tid",   # 파티션 0
        "ctid >= '(5000,0)'::tid AND ctid < '(10000,0)'::tid", # 파티션 1
        ...
        "ctid >= '(N,0)'::tid",                                 # 마지막 파티션
    ]
    모든 파티션은 Spark에서 병렬로 실행된다.
    """
    pages_per_part = max(1, math.ceil(total_pages / n_partitions))
    predicates = []
    for i in range(n_partitions):
        lo = i * pages_per_part
        if i < n_partitions - 1:
            hi = (i + 1) * pages_per_part
            predicates.append(f"ctid >= '({lo},0)'::tid AND ctid < '({hi},0)'::tid")
        else:
            predicates.append(f"ctid >= '({lo},0)'::tid")

    props = {
        "user":      db_cfg.user,
        "password":  db_cfg.password,
        "driver":    db_cfg.jdbc_driver,
        "fetchsize": str(fetch_size),
    }
    if db_cfg.session_init_statement:
        props["sessionInitStatement"] = db_cfg.session_init_statement

    return spark.read.jdbc(
        url=db_cfg.jdbc_url,
        table=db_cfg.fqn(db, table),
        predicates=predicates,
        properties=props,
    )


def _read_jdbc_ctid_range(
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    db: str,
    table: str,
    lo_page: int,
    hi_page: int | None,
    n_partitions: int,
    fetch_size: int = 10_000,
):
    """
    PostgreSQL ctid 기반 페이지 구간 읽기.

    lo_page 이상 hi_page 미만 페이지만 읽는다.
    hi_page=None이면 lo_page 이상 끝까지 (마지막 배치용).
    """
    if hi_page is not None:
        span = hi_page - lo_page
    else:
        span = n_partitions  # open-ended: 파티션 수만큼만 분할
    actual_n = max(1, min(n_partitions, span))
    pages_per_part = max(1, math.ceil(span / actual_n))

    predicates = []
    for i in range(actual_n):
        p_lo = lo_page + i * pages_per_part
        p_hi = lo_page + (i + 1) * pages_per_part
        is_last = (i == actual_n - 1)
        if is_last and hi_page is None:
            predicates.append(f"ctid >= '({p_lo},0)'::tid")
        elif is_last:
            predicates.append(f"ctid >= '({p_lo},0)'::tid AND ctid < '({hi_page},0)'::tid")
        else:
            predicates.append(f"ctid >= '({p_lo},0)'::tid AND ctid < '({p_hi},0)'::tid")

    props = {
        "user":      db_cfg.user,
        "password":  db_cfg.password,
        "driver":    db_cfg.jdbc_driver,
        "fetchsize": str(fetch_size),
    }
    if db_cfg.session_init_statement:
        props["sessionInitStatement"] = db_cfg.session_init_statement

    return spark.read.jdbc(
        url=db_cfg.jdbc_url,
        table=db_cfg.fqn(db, table),
        predicates=predicates,
        properties=props,
    )


def _read_jdbc_hash_batch(
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    db: str,
    table: str,
    hash_col: str,
    bucket: int,
    total_buckets: int,
    fetch_size: int = 10_000,
):
    """MySQL 텍스트 PK용: hash(col) % total = bucket 조건으로 한 버킷만 읽기."""
    where = db_cfg.hash_mod_expr(hash_col, total_buckets, bucket)
    query = f"(SELECT * FROM {db_cfg.fqn(db, table)} WHERE {where}) AS t"
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
            "%s 배치 %d/%d  %s.%s  pk %d ~ %d",
            tag, batch_num, total_batches, task.db, task.table, batch_lo, batch_hi,
        )
        df = _read_jdbc_range(spark, db_cfg, task.db, task.table,
                              task.pk_column, batch_lo, batch_hi,
                              num_partitions=task.num_partitions)
        ns = _iceberg_namespace(task)
        if batch_num == 1:
            minio.write_iceberg(df, ns, task.table)
        else:
            minio.append_iceberg(df, ns, task.table)
        batch_lo = batch_hi + 1

    log.info("%s 배치 적재 완료  %s.%s  (%d배치)", tag, task.db, task.table, batch_num)




def _load_in_hash_batches(
    tag: str,
    task: "TableTask",
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    minio: MinIOConnector,
) -> None:
    """MySQL 텍스트 PK 테이블용 해시 배치 적재 (폴백)."""
    n_buckets = math.ceil(task.total_rows / task.batch_size)
    log.info(
        "%s hash 배치 적재 시작  %s.%s  (rows=%d, buckets=%d, hash_col=%s)",
        tag, task.db, task.table, task.total_rows, n_buckets, task.hash_partition_col,
    )
    ns = _iceberg_namespace(task)
    for bucket in range(n_buckets):
        log.info("%s hash 배치 %d/%d", tag, bucket + 1, n_buckets)
        df = _read_jdbc_hash_batch(
            spark, db_cfg, task.db, task.table,
            task.hash_partition_col, bucket, n_buckets,
        )
        if bucket == 0:
            minio.write_iceberg(df, ns, task.table)
        else:
            minio.append_iceberg(df, ns, task.table)
    log.info("%s hash 배치 적재 완료  %s.%s  (%d버킷)", tag, task.db, task.table, n_buckets)


def _load_in_ctid_batches(
    tag: str,
    task: "TableTask",
    spark: SparkSession,
    db_cfg: BaseDBConfig,
    minio: MinIOConnector,
) -> None:
    """PostgreSQL ctid 기반 페이지 범위 순차 배치 적재."""
    total_pages = task.upper_bound
    pages_per_batch = task.ctid_batch_pages
    total_batches = math.ceil(total_pages / pages_per_batch)
    log.info(
        "%s ctid 배치 적재 시작  %s.%s  (rows=%d, pages=%d, batch_pages=%d, batches=%d)",
        tag, task.db, task.table,
        task.total_rows, total_pages, pages_per_batch, total_batches,
    )
    ns = _iceberg_namespace(task)
    for batch_num in range(total_batches):
        lo_page = batch_num * pages_per_batch
        hi_page = min((batch_num + 1) * pages_per_batch, total_pages)
        is_last = (batch_num == total_batches - 1)
        log.info(
            "%s ctid 배치 %d/%d  %s.%s  pages %d ~ %s",
            tag, batch_num + 1, total_batches, task.db, task.table, lo_page,
            "end" if is_last else str(hi_page),
        )
        n_parts = max(1, min(task.num_partitions, hi_page - lo_page))
        df = _read_jdbc_ctid_range(
            spark, db_cfg, task.db, task.table,
            lo_page, None if is_last else hi_page, n_parts,
        )
        if batch_num == 0:
            minio.write_iceberg(df, ns, task.table)
        else:
            minio.append_iceberg(df, ns, task.table)
    log.info("%s ctid 배치 적재 완료  %s.%s  (%d배치)", tag, task.db, task.table, total_batches)


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
                "%s 시작  %s.%s  (rows=%d, partitions=%d, pk=%s, hash_col=%s, batch_size=%s)",
                tag, task.db, task.table,
                task.total_rows, task.num_partitions,
                task.pk_column or "-",
                task.hash_partition_col or "-",
                task.batch_size if task.batch_size > 0 else "없음",
            )
            if task.pk_column == "__ctid__":
                if task.ctid_batch_pages > 0:
                    _load_in_ctid_batches(tag, task, spark, db_cfg, minio)
                else:
                    log.info(
                        "%s ctid 병렬 읽기  %s.%s  (rows=%d, pages=%d, partitions=%d)",
                        tag, task.db, task.table, task.total_rows,
                        task.upper_bound, task.num_partitions,
                    )
                    df = _read_jdbc_ctid_parallel(
                        spark, db_cfg, task.db, task.table,
                        task.upper_bound, task.num_partitions,
                    )
                    minio.write_iceberg(df, _iceberg_namespace(task), task.table)
            elif task.batch_size > 0 and task.pk_column:
                _load_in_batches(tag, task, spark, db_cfg, minio)
            elif task.batch_size > 0 and task.hash_partition_col:
                _load_in_hash_batches(tag, task, spark, db_cfg, minio)
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
    ctid_pages_map: dict | None = None,
    single_shot: bool = False,
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

            # __ctid__ sentinel: PostgreSQL ctid 페이지 범위 배치
            # lo=hi=-1  sentinel: MySQL 텍스트 PK 해시 배치
            hash_col: str | None = None
            if pk_col == "__ctid__":
                # ctid 병렬 읽기: num_partitions 개의 페이지 구간을 Spark가 병렬로 읽음
                num_partitions = min(
                    math.ceil(total_rows / cfg.chunk_size),
                    cfg.num_threads,
                )
                num_partitions = max(num_partitions, 1)
                # single_shot이면 배치 없이 전체 한 번에 읽기
                is_large = total_rows >= cfg.large_table_threshold
                if not single_shot and is_large and hi > 0:
                    rows_per_page = max(1, math.ceil(total_rows / hi))
                    ctid_batch_pages = max(1, math.ceil(cfg.large_table_batch_size / rows_per_page))
                else:
                    ctid_batch_pages = 0
                tasks.append(TableTask(
                    db=db, table=table,
                    total_rows=total_rows,
                    pk_column="__ctid__",
                    lower_bound=lo,          # 0
                    upper_bound=hi,          # relpages
                    num_partitions=num_partitions,
                    result=result_map[(db, table)],
                    batch_size=0,
                    pg_database=pg_database,
                    ctid_batch_pages=ctid_batch_pages,
                ))
                continue

            if pk_col and hi == -1:
                hash_col = pk_col
                pk_col, lo, hi = None, 0, 0

            if pk_col and hi > lo:
                num_partitions = min(
                    math.ceil(total_rows / cfg.chunk_size),
                    cfg.num_threads,
                )
                num_partitions = max(num_partitions, 1)
            else:
                num_partitions = 1

            is_large = total_rows >= cfg.large_table_threshold

            # PK 범위가 batch_size보다 작으면 배치를 나눌 수 없음 (batches=1이 되어 무의미)
            # PostgreSQL인 경우 실제 relpages를 이용해 ctid 배치로 폴백
            pk_range_too_narrow = pk_col and (hi - lo + 1) < cfg.large_table_batch_size

            if not single_shot and is_large and pk_range_too_narrow and pg_database is not None:
                actual_pages = (ctid_pages_map or {}).get((db, table), 0)
                if actual_pages > 0:
                    rows_per_page    = max(1, math.ceil(total_rows / actual_pages))
                    ctid_batch_pages = max(1, math.ceil(cfg.large_table_batch_size / rows_per_page))
                    log.info(
                        "%s.%s: 숫자 PK 범위(%d)가 batch_size(%d)보다 작음 → ctid 배치 폴백 (pages=%d, batch_pages=%d)",
                        db, table, hi - lo + 1, cfg.large_table_batch_size,
                        actual_pages, ctid_batch_pages,
                    )
                    tasks.append(TableTask(
                        db=db, table=table,
                        total_rows=total_rows,
                        pk_column="__ctid__",
                        lower_bound=0,
                        upper_bound=actual_pages,
                        num_partitions=num_partitions,
                        result=result_map[(db, table)],
                        batch_size=0,
                        pg_database=pg_database,
                        ctid_batch_pages=ctid_batch_pages,
                    ))
                    continue
                # relpages 정보 없으면 단건 읽기로 진행 (fallthrough)

            if not single_shot and is_large and pk_col and hi > lo:
                batch_size = cfg.large_table_batch_size   # 숫자 PK 범위 배치
            elif not single_shot and is_large and hash_col:
                batch_size = cfg.large_table_batch_size   # 텍스트 PK 해시 배치
            else:
                batch_size = 0

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
                hash_partition_col=hash_col,
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
    cancel_event: threading.Event | None = None,
) -> None:
    """
    Spark JDBC 기반 병렬 적재 실행.

    소형 테이블 (num_threads 워커) + 대형 테이블 (large_table_workers 워커) 를
    동시에 병렬 실행한다.
    2차: 실패 테이블 재시도. 재시도도 실패 시 SystemExit(1).

    cancel_event가 주어지면 이를 stop_event로 사용하여 외부에서 중단 가능.
    """
    stop_event = cancel_event if cancel_event is not None else threading.Event()

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


def run_compaction(
    tasks: list[TableTask],
    spark: SparkSession,
    minio: MinIOConnector,
    cancel_event: threading.Event | None = None,
) -> None:
    """배치 적재 테이블의 Iceberg 소형 파일을 순차 병합(compaction)."""
    targets = [t for t in tasks if t.batch_size > 0 or t.ctid_batch_pages > 0]
    if not targets:
        log.info("compaction 대상 없음 (배치 적재 테이블 없음)")
        return
    log.info(
        "Iceberg compaction 시작  (%d개 테이블): %s",
        len(targets), [f"{t.db}.{t.table}" for t in targets],
    )
    for task in targets:
        if cancel_event and cancel_event.is_set():
            log.info("compaction 중단 (취소 요청)")
            return
        minio.compact_iceberg(spark, _iceberg_namespace(task), task.table)
    log.info("Iceberg compaction 전체 완료")
