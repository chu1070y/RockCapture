from pyspark.sql import DataFrame
from core.config import MinIOConfig, IcebergConfig
from core.logger import get_logger

log = get_logger(__name__)


def _ensure_namespace(spark, catalog: str, namespace: list[str]) -> None:
    """
    Iceberg catalog에 namespace 계층이 없으면 단계별로 생성 (멱등).

    예) namespace=["mydb", "public"] 이면
        CREATE NAMESPACE IF NOT EXISTS catalog.`mydb`
        CREATE NAMESPACE IF NOT EXISTS catalog.`mydb`.`public`
    """
    for depth in range(1, len(namespace) + 1):
        ns_parts = ".".join(f"`{p}`" for p in namespace[:depth])
        try:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.{ns_parts}")
        except Exception as e:
            log.warning("namespace 생성 중 오류 (무시됨): %s", e)


class MinIOConnector:
    """MinIO(S3A) 위에 Iceberg 테이블 적재."""

    def __init__(self, cfg: MinIOConfig, iceberg_cfg: IcebergConfig):
        self._iceberg_cfg = iceberg_cfg
        log.debug(
            "MinIOConnector 초기화  (endpoint=%s, bucket=%s, catalog=%s)",
            cfg.endpoint, cfg.bucket, iceberg_cfg.catalog_name,
        )

    def _fqn(self, namespace: list[str], table: str) -> str:
        """
        Iceberg 테이블 완전 식별자 반환.

        MySQL      namespace=["mydb"]          → catalog.`mydb`.`table`
        PostgreSQL namespace=["mydb","public"] → catalog.`mydb`.`public`.`table`
        """
        ns_parts = ".".join(f"`{p}`" for p in namespace)
        return f"`{self._iceberg_cfg.catalog_name}`.{ns_parts}.`{table}`"

    def write_iceberg(self, df: DataFrame, namespace: list[str], table: str) -> None:
        """DataFrame을 Iceberg 테이블로 적재 (createOrReplace)."""
        fqn = self._fqn(namespace, table)
        log.info("Iceberg 적재 시작  (table=%s)", fqn)
        _ensure_namespace(df.sparkSession, self._iceberg_cfg.catalog_name, namespace)
        df.writeTo(fqn).createOrReplace()
        log.info("Iceberg 적재 완료  (table=%s)", fqn)

    def append_iceberg(self, df: DataFrame, namespace: list[str], table: str) -> None:
        """DataFrame을 기존 Iceberg 테이블에 append. 배치 적재 2번째 청크부터 사용."""
        fqn = self._fqn(namespace, table)
        log.debug("Iceberg append  (table=%s)", fqn)
        df.writeTo(fqn).append()
        log.debug("Iceberg append 완료  (table=%s)", fqn)

    def compact_iceberg(self, spark, namespace: list[str], table: str) -> None:
        """rewrite_data_files로 배치 적재로 생긴 소형 파일을 128MB 단위로 병합."""
        catalog   = self._iceberg_cfg.catalog_name
        table_ref = ".".join(namespace + [table])
        fqn       = self._fqn(namespace, table)
        log.info("Iceberg compaction 시작  (table=%s)", fqn)
        try:
            spark.sql(
                f"CALL `{catalog}`.system.rewrite_data_files("
                f"table => '{table_ref}', "
                f"options => map('target-file-size-bytes', '268435456')"
                f")"
            )
            log.info("Iceberg compaction 완료  (table=%s)", fqn)
        except Exception as e:
            log.warning("Iceberg compaction 실패 (무시됨)  (table=%s): %s", fqn, e)
