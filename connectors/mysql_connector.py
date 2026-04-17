from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue

import pymysql
import pymysql.cursors

from core.config import MySQLConfig
from connectors.base_connector import BaseDBConnector
from connectors.metadata_writer import SnapshotMetadata, TableResult
from core.logger import get_logger

log = get_logger(__name__)

SYSTEM_DATABASES = {"information_schema", "performance_schema", "mysql", "sys"}


class MySQLConnector(BaseDBConnector):
    """
    pymysql 기반 MySQL 커넥터 (메타데이터 전용).

    메인 커넥션: START TRANSACTION WITH CONSISTENT SNAPSHOT 으로 시작.
    워커 커넥션: FTWRL 유지 중 open_worker_connections() 로 확보,
                 통계 수집 후 close_worker_connections() 로 종료.

    사용법:
        with MySQLConnector(cfg) as mysql:
            mysql.lock_tables_for_snapshot()
            snapshot_meta = mysql.init_snapshot(db_tables)
            mysql.open_worker_connections(workers=4)
            mysql.unlock_tables()
            row_counts, pk_info = mysql.collect_all_table_stats(db_tables)
            mysql.close_worker_connections()
    """

    def __init__(self, cfg: MySQLConfig):
        self._cfg = cfg
        self._conn: pymysql.Connection | None = None
        self._worker_pool: Queue | None = None
        self._worker_conns: list[pymysql.Connection] = []
        log.debug("MySQLConnector 생성  (host=%s)", cfg.host)

    # ── 커넥션 생명주기 ───────────────────────────────────────

    def connect(self) -> None:
        log.info("MySQL 연결 시작  (host=%s)", self._cfg.host)
        self._conn = pymysql.connect(
            host=self._cfg.host,
            port=int(self._cfg.port),
            user=self._cfg.user,
            password=self._cfg.password,
            autocommit=False,
            cursorclass=pymysql.cursors.Cursor,
        )
        with self._conn.cursor() as cur:
            cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        log.info("MySQL 연결 완료")

    def disconnect(self) -> None:
        if self._conn:
            self._conn.commit()
            self._conn.close()
            self._conn = None
            log.info("MySQL 연결 종료")

    def __enter__(self) -> "MySQLConnector":  # type: ignore[override]
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, _) -> None:
        if exc_type:
            log.exception("예외 발생 — ROLLBACK  (%s)", exc_val)
            if self._conn:
                self._conn.rollback()
                self._conn.close()
                self._conn = None
        else:
            self.disconnect()

    def _assert_connected(self) -> None:
        if self._conn is None:
            raise RuntimeError("MySQL 커넥션이 없습니다. connect() 또는 with 구문을 먼저 사용하세요.")

    # ── DB / 테이블 목록 ──────────────────────────────────────

    def list_databases(self) -> list[str]:
        self._assert_connected()
        with self._conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            dbs = [r[0] for r in cur.fetchall() if r[0] not in SYSTEM_DATABASES]
        log.info("데이터베이스 %d개  %s", len(dbs), dbs)
        return dbs

    def list_tables(self, database: str) -> list[str]:
        self._assert_connected()
        with self._conn.cursor() as cur:
            cur.execute(f"SHOW TABLES IN `{database}`")
            tables = [r[0] for r in cur.fetchall()]
        log.info("  └─ %s: 테이블 %d개  %s", database, len(tables), tables)
        return tables

    # ── 스냅숏 캡처 ───────────────────────────────────────────

    def lock_tables_for_snapshot(self) -> None:
        """FLUSH TABLES WITH READ LOCK 획득. init_snapshot() 후 unlock_tables() 호출 필요."""
        self._assert_connected()
        with self._conn.cursor() as cur:
            cur.execute("FLUSH TABLES WITH READ LOCK")
        log.info("FTWRL 획득 — 쓰기 차단됨")

    def init_snapshot(self, db_tables: dict[str, list[str]]) -> SnapshotMetadata:
        """현재 binlog 위치를 캡처하여 SnapshotMetadata 반환."""
        self._assert_connected()
        row = None
        with self._conn.cursor() as cur:
            for query in ("SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"):
                try:
                    cur.execute(query)
                    row = cur.fetchone()
                    if row:
                        break
                except Exception:
                    pass
        if not row:
            raise RuntimeError("binlog 위치 조회 실패. log_bin 활성화 여부를 확인하세요.")

        meta = SnapshotMetadata(
            snapshot_at=datetime.now().isoformat(timespec="seconds"),
            db_type="mysql",
            binlog_file=row[0],
            binlog_position=row[1],
            binlog_do_db=row[2],
            binlog_ignore_db=row[3],
            executed_gtid_set=row[4] if len(row) > 4 else "",
            tables=[
                TableResult(database=db, name=table)
                for db, tables in db_tables.items()
                for table in tables
            ],
        )
        log.info("스냅숏 캡처  (binlog=%s:%d)", meta.binlog_file, meta.binlog_position)
        return meta

    def unlock_tables(self) -> None:
        """UNLOCK TABLES — lock_tables_for_snapshot() 후 반드시 호출."""
        self._assert_connected()
        with self._conn.cursor() as cur:
            cur.execute("UNLOCK TABLES")
        log.info("UNLOCK TABLES — 쓰기 재개")

    # ── 워커 커넥션 풀 ────────────────────────────────────────

    def open_worker_connections(self, workers: int = 4) -> None:
        """
        FTWRL 유지 중에 호출해야 함.
        각 커넥션이 START TRANSACTION WITH CONSISTENT SNAPSHOT을 실행하여
        FTWRL 시점과 동일한 read view를 확보한 뒤 unlock_tables()를 호출한다.
        """
        self._worker_conns = []
        for _ in range(workers):
            conn = pymysql.connect(
                host=self._cfg.host,
                port=int(self._cfg.port),
                user=self._cfg.user,
                password=self._cfg.password,
                autocommit=False,
                cursorclass=pymysql.cursors.Cursor,
            )
            with conn.cursor() as cur:
                cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            self._worker_conns.append(conn)
        self._worker_pool = Queue()
        for conn in self._worker_conns:
            self._worker_pool.put(conn)
        log.info("워커 커넥션 %d개 생성  (CONSISTENT SNAPSHOT)", workers)

    def close_worker_connections(self) -> None:
        """워커 커넥션 전체 종료. 통계 수집 완료 후 호출할 것."""
        for conn in self._worker_conns:
            conn.close()
        self._worker_conns = []
        self._worker_pool = None
        log.info("워커 커넥션 종료")

    # ── 통계 수집 ─────────────────────────────────────────────

    def _query_table_stats(self, db: str, table: str, conn: pymysql.Connection) -> dict:
        """워커 커넥션 하나로 단일 테이블의 행 수·PK·MIN/MAX를 조회."""
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{db}`.`{table}`")
            count = cur.fetchone()[0]
        log.debug("행 수  (%s.%s = %d)", db, table, count)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT k.COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE k
                JOIN information_schema.COLUMNS c
                  ON c.TABLE_SCHEMA = k.TABLE_SCHEMA
                 AND c.TABLE_NAME   = k.TABLE_NAME
                 AND c.COLUMN_NAME  = k.COLUMN_NAME
                WHERE k.TABLE_SCHEMA    = %s
                  AND k.TABLE_NAME      = %s
                  AND k.CONSTRAINT_NAME = 'PRIMARY'
                  AND c.DATA_TYPE IN (
                      'tinyint','smallint','mediumint','int','bigint'
                  )
                ORDER BY k.ORDINAL_POSITION
                LIMIT 1
                """,
                (db, table),
            )
            row = cur.fetchone()
        pk = row[0] if row else None
        log.debug("숫자형 PK  (%s.%s = %s)", db, table, pk)

        lo, hi = 0, 0
        if pk:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MIN(`{pk}`), MAX(`{pk}`) FROM `{db}`.`{table}`")
                row = cur.fetchone()
            lo = int(row[0]) if row[0] is not None else 0
            hi = int(row[1]) if row[1] is not None else 0
            log.debug("MIN/MAX  (%s.%s.%s = %d ~ %d)", db, table, pk, lo, hi)

        return {"db": db, "table": table, "count": count, "pk": pk, "lo": lo, "hi": hi}

    def collect_all_table_stats(
        self, db_tables: dict[str, list[str]]
    ) -> tuple[dict, dict]:
        """
        워커 커넥션 풀로 모든 테이블의 행 수·PK·MIN/MAX를 병렬 수집.

        Returns:
            row_counts : {(db, table): int}
            pk_info    : {(db, table): (pk_column, lower_bound, upper_bound)}
        """
        if not self._worker_pool:
            raise RuntimeError("워커 커넥션이 없습니다. open_worker_connections()를 먼저 호출하세요.")

        pairs = [(db, t) for db, tables in db_tables.items() for t in tables]
        row_counts: dict = {}
        pk_info: dict = {}

        def fetch(db: str, table: str) -> dict:
            conn = self._worker_pool.get()
            try:
                return self._query_table_stats(db, table, conn)
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

        return row_counts, pk_info
