from pyspark.sql import DataFrame
from core.config import MinIOConfig, IcebergConfig
from core.logger import get_logger

log = get_logger(__name__)


def _ensure_namespace(spark, catalog: str, database: str) -> None:
    """Iceberg catalog에 namespace가 없으면 생성 (멱등)."""
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`{database}`")
    except Exception as e:
        log.warning("namespace 생성 중 오류 (무시됨): %s", e)


class MinIOConnector:
    """MinIO(S3A) 위에 Iceberg 테이블 적재."""

    def __init__(self, cfg: MinIOConfig, iceberg_cfg: IcebergConfig):
        self._cfg = cfg
        self._iceberg_cfg = iceberg_cfg
        log.debug(
            "MinIOConnector 초기화  (endpoint=%s, bucket=%s, catalog=%s)",
            cfg.endpoint, cfg.bucket, iceberg_cfg.catalog_name,
        )

    def _fqn(self, database: str, table: str) -> str:
        return f"`{self._iceberg_cfg.catalog_name}`.`{database}`.`{table}`"

    def write_iceberg(self, df: DataFrame, database: str, table: str) -> None:
        """DataFrame을 Iceberg 테이블로 적재 (createOrReplace)."""
        fqn = self._fqn(database, table)
        log.info("Iceberg 적재 시작  (table=%s)", fqn)
        _ensure_namespace(df.sparkSession, self._iceberg_cfg.catalog_name, database)
        df.writeTo(fqn).createOrReplace()
        log.info("Iceberg 적재 완료  (table=%s)", fqn)

    def append_iceberg(self, df: DataFrame, database: str, table: str) -> None:
        """DataFrame을 기존 Iceberg 테이블에 append. 배치 적재 2번째 청크부터 사용."""
        fqn = self._fqn(database, table)
        log.debug("Iceberg append  (table=%s)", fqn)
        df.writeTo(fqn).append()
        log.debug("Iceberg append 완료  (table=%s)", fqn)
