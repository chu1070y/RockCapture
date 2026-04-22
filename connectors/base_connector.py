"""
base_connector.py

MySQL / PostgreSQL 커넥터가 공통으로 구현해야 하는 인터페이스 정의.
main.py 및 pipeline_runner.py는 이 인터페이스에만 의존하여 DB 종류에 무관하게 동작한다.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from connectors.metadata_writer import SnapshotMetadata


class BaseDBConnector(ABC):
    """DB 커넥터 공통 인터페이스."""

    # ── 커넥션 생명주기 ────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    def __enter__(self) -> "BaseDBConnector":
        self.connect()
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...

    # ── 스키마/테이블 목록 ─────────────────────────────────────────

    @abstractmethod
    def list_databases(self) -> list[str]:
        """
        접근 가능한 데이터베이스(MySQL) 또는 스키마(PostgreSQL) 목록을 반환.
        시스템 DB/스키마는 제외.
        """
        ...

    @abstractmethod
    def list_tables(self, database: str) -> list[str]:
        """지정한 데이터베이스(스키마) 내 테이블 목록을 반환."""
        ...

    @abstractmethod
    def refresh_stale_table_stats(
        self,
        db_tables: dict[str, list[str]],
        *,
        max_age_seconds: int,
    ) -> None:
        """오래된 테이블 통계만 선택적으로 갱신."""
        ...

    # ── 스냅숏 캡처 ───────────────────────────────────────────────

    @abstractmethod
    def lock_tables_for_snapshot(self) -> None:
        """
        일관된 스냅숏 확보를 위한 잠금 또는 격리 수준 설정.
        MySQL: FLUSH TABLES WITH READ LOCK
        PostgreSQL: BEGIN REPEATABLE READ + pg_export_snapshot()
        """
        ...

    @abstractmethod
    def init_snapshot(self, db_tables: dict[str, list[str]]) -> SnapshotMetadata:
        """현재 복제 위치(binlog / WAL LSN)를 캡처하여 SnapshotMetadata 반환."""
        ...

    @abstractmethod
    def open_worker_connections(self, workers: int = 4) -> None:
        """
        스냅숏과 동일한 read view를 가진 워커 커넥션 풀을 생성.
        lock_tables_for_snapshot() 호출 후, unlock_tables() 호출 전에 실행할 것.
        """
        ...

    @abstractmethod
    def unlock_tables(self) -> None:
        """
        잠금 해제 또는 no-op.
        MySQL: UNLOCK TABLES
        PostgreSQL: no-op (트랜잭션 격리로 대체)
        """
        ...

    @abstractmethod
    def collect_all_table_stats(
        self, db_tables: dict[str, list[str]]
    ) -> tuple[dict, dict]:
        """
        모든 테이블의 행 수·PK·MIN/MAX를 병렬 수집.

        Returns:
            row_counts : {(db, table): int}
            pk_info    : {(db, table): (pk_column, lower_bound, upper_bound)}
        """
        ...

    @abstractmethod
    def close_worker_connections(self) -> None:
        """워커 커넥션 전체 종료."""
        ...
