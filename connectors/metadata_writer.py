import io
import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

from core.logger import get_logger

log = get_logger(__name__)


@dataclass
class TableResult:
    database: str
    name: str
    row_count: int = -1


@dataclass
class SnapshotMetadata:
    snapshot_at: str
    db_type: str          # "mysql" | "postgresql"
    binlog_file: str      # MySQL: binlog 파일명 / PostgreSQL: WAL 세그먼트 파일명
    binlog_position: int  # MySQL: binlog 오프셋   / PostgreSQL: WAL 세그먼트 오프셋
    binlog_do_db: str     # MySQL 전용 (PostgreSQL: "")
    binlog_ignore_db: str # MySQL 전용 (PostgreSQL: "")
    executed_gtid_set: str  # MySQL: GTID 셋 / PostgreSQL: WAL LSN 전체 문자열
    tables: list[TableResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        """
        db_type에 맞는 키 이름으로 직렬화한 dict 반환.
        - MySQL    : binlog_file / binlog_position / binlog_do_db / binlog_ignore_db / executed_gtid_set
        - PostgreSQL: wal_file  / wal_offset       / wal_lsn
        """
        base = {
            "snapshot_at": self.snapshot_at,
            "db_type":     self.db_type,
            "tables":      [asdict(t) for t in self.tables],
        }
        if self.db_type == "postgresql":
            base.update({
                "wal_file":   self.binlog_file,
                "wal_offset": self.binlog_position,
                "wal_lsn":    self.executed_gtid_set,
            })
        else:
            base.update({
                "binlog_file":       self.binlog_file,
                "binlog_position":   self.binlog_position,
                "binlog_do_db":      self.binlog_do_db,
                "binlog_ignore_db":  self.binlog_ignore_db,
                "executed_gtid_set": self.executed_gtid_set,
            })
        return base


class MetadataWriter:
    """스냅숏 메타데이터를 로컬 파일 / MinIO에 저장"""

    def __init__(self, output_dir: str | None = None):
        self._output_dir = Path(output_dir or os.environ.get("METADATA_DIR", "metadata"))

    @staticmethod
    def _file_name(meta: SnapshotMetadata) -> str:
        """'{db_type}_{YYYYMMDD_HHMMSS}.json' 형식의 파일명 반환."""
        date_str = datetime.fromisoformat(meta.snapshot_at).strftime("%Y%m%d_%H%M%S")
        return f"{meta.db_type}_{date_str}.json"

    def save_local(self, meta: SnapshotMetadata) -> Path:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        path = self._output_dir / self._file_name(meta)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(meta.to_dict(), f, ensure_ascii=False, indent=2)
        log.info("메타데이터 로컬 저장 완료  (path=%s)", path)
        return path

    def save_replication_status_to_minio(self, meta: SnapshotMetadata, minio_cfg) -> None:
        """복제 위치(binlog 또는 WAL) 상태값만 추출하여 MinIO에 저장.

        저장 경로:
          - MySQL      : snapshot_status/mysql_{YYYYMMDD}_binlog_status.json
          - PostgreSQL : snapshot_status/postgresql_{YYYYMMDD}_wal_status.json
        """
        import boto3
        from botocore.config import Config

        date_str = datetime.fromisoformat(meta.snapshot_at).strftime("%Y%m%d_%H%M%S")

        if meta.db_type == "postgresql":
            payload = {
                "snapshot_at": meta.snapshot_at,
                "wal_file":    meta.binlog_file,
                "wal_offset":  meta.binlog_position,
                "wal_lsn":     meta.executed_gtid_set,
            }
            object_key = f"snapshot_status/postgresql_{date_str}_wal_status.json"
        else:
            payload = {
                "snapshot_at":       meta.snapshot_at,
                "binlog_file":       meta.binlog_file,
                "binlog_position":   meta.binlog_position,
                "binlog_do_db":      meta.binlog_do_db,
                "binlog_ignore_db":  meta.binlog_ignore_db,
                "executed_gtid_set": meta.executed_gtid_set,
            }
            object_key = f"snapshot_status/mysql_{date_str}_binlog_status.json"

        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        s3 = boto3.client(
            "s3",
            endpoint_url=minio_cfg.endpoint,
            aws_access_key_id=minio_cfg.access_key,
            aws_secret_access_key=minio_cfg.secret_key,
            region_name=minio_cfg.region,
            config=Config(signature_version="s3v4"),
        )
        s3.put_object(
            Bucket=minio_cfg.bucket,
            Key=object_key,
            Body=io.BytesIO(body),
            ContentType="application/json",
        )
        log.info(
            "복제 상태 MinIO 저장 완료  (s3://%s/%s)",
            minio_cfg.bucket, object_key,
        )
