import io
import json
from dataclasses import dataclass, field, asdict
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
    binlog_file: str
    binlog_position: int
    binlog_do_db: str
    binlog_ignore_db: str
    executed_gtid_set: str
    tables: list[TableResult] = field(default_factory=list)


class MetadataWriter:
    """스냅숏 메타데이터를 로컬 파일 / MinIO에 저장"""

    FILE_NAME = "metadata.json"

    def __init__(self, output_dir: str = "metadata"):
        self._output_dir = Path(output_dir)

    def save_local(self, meta: SnapshotMetadata) -> Path:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        path = self._output_dir / self.FILE_NAME
        with open(path, "w", encoding="utf-8") as f:
            json.dump(asdict(meta), f, ensure_ascii=False, indent=2)
        log.info("메타데이터 로컬 저장 완료  (path=%s)", path)
        return path

    def save_to_minio(self, meta: SnapshotMetadata, spark, base_path: str) -> None:
        """MinIO에 단일 JSON 파일로 저장"""
        from pyspark.sql.types import StringType

        json_str = json.dumps(asdict(meta), ensure_ascii=False)
        df = spark.createDataFrame([json_str], StringType())
        path = f"{base_path.rstrip('/')}/_metadata/"
        df.coalesce(1).write.mode("overwrite").text(path)
        log.info("메타데이터 MinIO 저장 완료  (path=%s)", path)

    def save_binlog_status_to_minio(self, meta: SnapshotMetadata, minio_cfg) -> None:
        """binlog 상태값만 추출하여 MinIO에 binlog_status.json으로 저장."""
        import boto3
        from botocore.config import Config

        payload = {
            "snapshot_at":       meta.snapshot_at,
            "binlog_file":       meta.binlog_file,
            "binlog_position":   meta.binlog_position,
            "binlog_do_db":      meta.binlog_do_db,
            "binlog_ignore_db":  meta.binlog_ignore_db,
            "executed_gtid_set": meta.executed_gtid_set,
        }
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        object_key = "_metadata/binlog_status.json"

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
            "binlog 상태 MinIO 저장 완료  (s3://%s/%s)",
            minio_cfg.bucket, object_key,
        )
