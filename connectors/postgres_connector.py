"""
postgres_connector.py

psycopg2 기반 PostgreSQL 커넥터 (메타데이터 전용).

MySQL 커넥터와의 주요 차이:
  - 스냅숏: FLUSH TABLES WITH READ LOCK 대신 REPEATABLE READ 격리 수준 + pg_export_snapshot()
  - 스키마: MySQL의 "데이터베이스" 개념을 PostgreSQL "스키마"로 매핑
            (하나의 PostgreSQL 데이터베이스에 접속 후 스키마 목록을 사용)
  - 복제 위치: binlog 대신 WAL LSN (pg_current_wal_lsn())
  - unlock_tables(): no-op (격리 수준으로 일관성 확보)

사용법:
    with PostgreSQLConnector(cfg) as pg:
        pg.lock_tables_for_snapshot()
        snapshot_meta = pg.init_snapshot(db_tables)
        pg.open_worker_connections(workers=4)
        pg.unlock_tables()          # no-op
        row_counts, pk_info = pg.collect_all_table_stats(db_tables)
        pg.close_worker_connections()
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue

import psycopg2

from core.config import PostgreSQLConfig
from connectors.base_connector import BaseDBConnector
from connectors.metadata_writer import SnapshotMetadata, TableResult
from core.logger import get_logger

log = get_logger(__name__)

# 시스템 스키마 (적재 대상에서 제외)
SYSTEM_SCHEMAS = {
    "information_schema",
    "pg_catalog",
    "pg_toast",
    "pg_temp_1",
    "pg_toast_temp_1",
}


def _is_system_schema(name: str) -> bool:
    return name in SYSTEM_SCHEMAS or name.startswith("pg_")


class PostgreSQLConnector(BaseDBConnector):
    """
    psycopg2 기반 PostgreSQL 커넥터.

    - 메인 커넥션: REPEATABLE READ 트랜잭션 시작 후 pg_export_snapshot()으로 스냅숏 ID 획득.
    - 워커 커넥션: SET TRANSACTION SNAPSHOT 'snap_id'로 동일 스냅숏 가져오기.
    """

    def __init__(self, cfg: PostgreSQLConfig):
        self._cfg = cfg
        self._conn: psycopg2.extensions.connection | None = None
        self._snapshot_id: str | None = None
        self._worker_pool: Queue | None = None
        self._worker_conns: list[psycopg2.extensions.connection] = []
        log.debug("PostgreSQLConnector 생성  (host=%s, db=%s)", cfg.host, cfg.database)

    # ── 커넥션 생명주기 ───────────────────────────────────────────

    def _make_conn(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self._cfg.host,
            port=int(self._cfg.port),
            user=self._cfg.user,
            password=self._cfg.password,
            dbname=self._cfg.database,
        )

    def connect(self) -> None:
        log.info("PostgreSQL 연결 시작  (host=%s, db=%s)", self._cfg.host, self._cfg.database)
        self._conn = self._make_conn()
        # list_databases / list_tables 단계는 autocommit=True로 각각 독립 실행.
        # lock_tables_for_snapshot() 에서 False로 전환해 REPEATABLE READ 트랜잭션을 시작한다.
        self._conn.autocommit = True
        log.info("PostgreSQL 연결 완료")

    def disconnect(self) -> None:
        if self._conn:
            try:
                if not self._conn.autocommit:
                    self._conn.rollback()   # 읽기 전용 트랜잭션 정리
            finally:
                self._conn.close()
                self._conn = None
                log.info("PostgreSQL 연결 종료")

    def __exit__(self, exc_type, exc_val, _) -> None:
        if exc_type:
            log.exception("예외 발생 — 연결 종료  (%s)", exc_val)
        self.disconnect()

    def _assert_connected(self) -> None:
        if self._conn is None:
            raise RuntimeError(
                "PostgreSQL 커넥션이 없습니다. connect() 또는 with 구문을 먼저 사용하세요."
            )

    # ── 스키마 / 테이블 목록 ──────────────────────────────────────

    def list_databases(self) -> list[str]:
        """
        사용자 스키마 목록을 반환 (시스템 스키마 제외).
        MySQL의 list_databases()에 대응 — PostgreSQL에서는 스키마를 "DB" 단위로 취급.
        """
        self._assert_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                ORDER BY schema_name
                """
            )
            schemas = [r[0] for r in cur.fetchall() if not _is_system_schema(r[0])]
        log.info("스키마 %d개  %s", len(schemas), schemas)
        return schemas

    def list_tables(self, database: str) -> list[str]:
        """지정 스키마 내 테이블 목록 반환."""
        self._assert_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (database,),
            )
            tables = [r[0] for r in cur.fetchall()]
        log.info("  └─ %s: 테이블 %d개  %s", database, len(tables), tables)
        return tables

    # ── 스냅숏 캡처 ───────────────────────────────────────────────

    def lock_tables_for_snapshot(self) -> None:
        """
        REPEATABLE READ 트랜잭션을 시작하고 pg_export_snapshot()으로 스냅숏 ID를 획득.
        이후 워커 커넥션이 동일 스냅숏을 import하여 일관된 read view를 공유한다.
        """
        self._assert_connected()
        # autocommit=False 전환 시 psycopg2가 다음 명령 전에 BEGIN을 자동 발행하므로
        # SET TRANSACTION ISOLATION LEVEL이 새 트랜잭션의 첫 구문이 된다.
        self._conn.autocommit = False
        with self._conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SELECT pg_export_snapshot()")
            self._snapshot_id = cur.fetchone()[0]
        log.info("PostgreSQL 스냅숏 획득  (snapshot_id=%s)", self._snapshot_id)

    def init_snapshot(self, db_tables: dict[str, list[str]]) -> SnapshotMetadata:
        """
        현재 WAL LSN을 캡처하여 SnapshotMetadata 반환.

        SnapshotMetadata 필드 매핑 (PostgreSQL):
          binlog_file      → WAL 세그먼트 파일명 (pg_walfile_name)
          binlog_position  → 세그먼트 내 오프셋 (int)
          binlog_do_db     → ""
          binlog_ignore_db → ""
          executed_gtid_set→ WAL LSN 전체 문자열 (예: "0/1628F68")
        """
        self._assert_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT pg_walfile_name(pg_current_wal_lsn()), "
                "       pg_walfile_name_offset(pg_current_wal_lsn()), "
                "       pg_current_wal_lsn()::text"
            )
            row = cur.fetchone()

        if not row:
            raise RuntimeError("WAL LSN 조회 실패. pg_current_wal_lsn() 권한을 확인하세요.")

        wal_file, wal_offset_row, lsn_str = row
        # pg_walfile_name_offset 는 (file_name, file_offset) 컴포짓 반환
        # psycopg2는 이를 튜플로 변환하므로 file_offset 추출
        wal_offset = wal_offset_row[1] if isinstance(wal_offset_row, tuple) else int(str(wal_offset_row).split(",")[-1].strip(")").strip())

        meta = SnapshotMetadata(
            snapshot_at=datetime.now().isoformat(timespec="seconds"),
            db_type="postgresql",
            binlog_file=wal_file,
            binlog_position=wal_offset,
            binlog_do_db="",
            binlog_ignore_db="",
            executed_gtid_set=lsn_str,
            tables=[
                TableResult(database=db, name=table)
                for db, tables in db_tables.items()
                for table in tables
            ],
        )
        log.info("WAL 스냅숏 캡처  (lsn=%s, file=%s, offset=%d)", lsn_str, wal_file, wal_offset)
        return meta

    def unlock_tables(self) -> None:
        """PostgreSQL은 명시적 잠금 해제 불필요 (no-op)."""
        log.info("PostgreSQL unlock_tables() — no-op (REPEATABLE READ 격리 수준으로 대체)")

    # ── 워커 커넥션 풀 ────────────────────────────────────────────

    def open_worker_connections(self, workers: int = 4) -> None:
        """
        lock_tables_for_snapshot() 으로 얻은 스냅숏 ID를 각 워커 커넥션에 import.
        메인 커넥션의 트랜잭션이 살아 있는 동안에만 스냅숏 import가 가능하므로
        unlock_tables() 전에 호출해야 한다.
        """
        if not self._snapshot_id:
            raise RuntimeError(
                "스냅숏 ID가 없습니다. lock_tables_for_snapshot()을 먼저 호출하세요."
            )

        self._worker_conns = []
        for _ in range(workers):
            conn = self._make_conn()
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute(f"SET TRANSACTION SNAPSHOT '{self._snapshot_id}'")
            self._worker_conns.append(conn)

        self._worker_pool = Queue()
        for conn in self._worker_conns:
            self._worker_pool.put(conn)
        log.info(
            "워커 커넥션 %d개 생성  (snapshot_id=%s)", workers, self._snapshot_id
        )

    def close_worker_connections(self) -> None:
        """워커 커넥션 전체 종료 및 메인 트랜잭션 정리."""
        for conn in self._worker_conns:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
        self._worker_conns = []
        self._worker_pool = None
        log.info("워커 커넥션 종료")

    # ── 통계 수집 ─────────────────────────────────────────────────

    def _query_table_stats(
        self, schema: str, table: str, conn: psycopg2.extensions.connection
    ) -> dict:
        """워커 커넥션 하나로 단일 테이블의 행 수·PK·MIN/MAX를 조회."""
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            count = cur.fetchone()[0]
        log.debug("행 수  (%s.%s = %d)", schema, table, count)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema    = kcu.table_schema
                JOIN information_schema.columns c
                    ON c.table_schema = kcu.table_schema
                   AND c.table_name   = kcu.table_name
                   AND c.column_name  = kcu.column_name
                WHERE tc.table_schema    = %s
                  AND tc.table_name      = %s
                  AND tc.constraint_type = 'PRIMARY KEY'
                  AND c.data_type IN ('smallint', 'integer', 'bigint')
                ORDER BY kcu.ordinal_position
                LIMIT 1
                """,
                (schema, table),
            )
            row = cur.fetchone()
        pk = row[0] if row else None
        log.debug("숫자형 PK  (%s.%s = %s)", schema, table, pk)

        lo, hi = 0, 0
        if pk:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT MIN("{pk}"), MAX("{pk}") FROM "{schema}"."{table}"'
                )
                row = cur.fetchone()
            lo = int(row[0]) if row and row[0] is not None else 0
            hi = int(row[1]) if row and row[1] is not None else 0
            log.debug("MIN/MAX  (%s.%s.%s = %d ~ %d)", schema, table, pk, lo, hi)

        # 숫자형 PK가 없으면 pg_class.relpages 로 ctid 페이지 범위 배치 정보 수집
        ctid_pages = 0
        if not pk:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.relpages
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                    """,
                    (schema, table),
                )
                row = cur.fetchone()
            ctid_pages = int(row[0]) if row and row[0] else 0
            log.debug("ctid 배치용 relpages  (%s.%s = %d)", schema, table, ctid_pages)

        return {"db": schema, "table": table, "count": count,
                "pk": pk, "lo": lo, "hi": hi, "ctid_pages": ctid_pages}

    def collect_all_table_stats(
        self, db_tables: dict[str, list[str]]
    ) -> tuple[dict, dict]:
        """
        워커 커넥션 풀로 모든 테이블의 행 수·PK·MIN/MAX를 병렬 수집.

        Returns:
            row_counts : {(schema, table): int}
            pk_info    : {(schema, table): (pk_column, lower_bound, upper_bound)}
        """
        if not self._worker_pool:
            raise RuntimeError(
                "워커 커넥션이 없습니다. open_worker_connections()를 먼저 호출하세요."
            )

        pairs = [(db, t) for db, tables in db_tables.items() for t in tables]
        row_counts: dict = {}
        pk_info: dict = {}

        def fetch(schema: str, table: str) -> dict:
            conn = self._worker_pool.get()
            try:
                return self._query_table_stats(schema, table, conn)
            finally:
                self._worker_pool.put(conn)

        with ThreadPoolExecutor(max_workers=len(self._worker_conns)) as executor:
            futures = {executor.submit(fetch, db, t): (db, t) for db, t in pairs}
            for future in as_completed(futures):
                r = future.result()
                key = (r["db"], r["table"])
                row_counts[key] = r["count"]
                if r["pk"]:
                    pk_info[key] = (r["pk"], r["lo"], r["hi"])
                elif r.get("ctid_pages", 0) > 0:
                    # 숫자형 PK 없음 → ctid 페이지 범위 배치
                    # sentinel: pk_column="__ctid__", lo=0, hi=relpages
                    pk_info[key] = ("__ctid__", 0, r["ctid_pages"])

        return row_counts, pk_info
