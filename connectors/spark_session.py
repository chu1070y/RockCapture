import os
import sys
from pyspark.sql import SparkSession
from core.config import BaseDBConfig, MinIOConfig, SparkConfig, IcebergConfig
from core.logger import get_logger

# Windows에서 Spark 실행 시 winutils.exe 경로 설정
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")

# Spark 워커가 현재 실행 중인 Python(venv)을 사용하도록 강제
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

log = get_logger(__name__)


class SparkSessionManager:
    """SparkSession 생성 및 생명주기 관리 (context manager 지원)"""

    def __init__(
        self,
        spark_cfg: SparkConfig,
        db_cfg: BaseDBConfig,
        minio_cfg: MinIOConfig,
        iceberg_cfg: IcebergConfig,
    ):
        self._spark_cfg = spark_cfg
        self._db_cfg = db_cfg
        self._minio_cfg = minio_cfg
        self._iceberg_cfg = iceberg_cfg
        self._session: SparkSession | None = None

    def build(self) -> SparkSession:
        log.info("SparkSession 생성 시작  (app=%s)", self._spark_cfg.app_name)
        log.debug("JDBC jar: %s", self._db_cfg.jdbc_jar_path)
        log.debug("extra packages: %s", self._spark_cfg.extra_packages)
        log.debug("MinIO endpoint: %s", self._minio_cfg.endpoint)

        catalog = self._iceberg_cfg.catalog_name

        builder = (
            SparkSession.builder
            .appName(self._spark_cfg.app_name)
            .config("spark.driver.memory", self._spark_cfg.driver_memory)
            .config("spark.jars", self._db_cfg.jdbc_jar_path)
            .config(
                "spark.jars.packages",
                ",".join(self._spark_cfg.extra_packages),
            )
            # S3A → MinIO
            .config("spark.hadoop.fs.s3a.endpoint",
                    self._minio_cfg.endpoint)
            .config("spark.hadoop.fs.s3a.access.key",
                    self._minio_cfg.access_key)
            .config("spark.hadoop.fs.s3a.secret.key",
                    self._minio_cfg.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                    str(self._minio_cfg.ssl_enabled).lower())
            .config("spark.hadoop.fs.s3a.endpoint.region",
                    self._minio_cfg.region)
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            # Direct Memory OOM 방지: bytebuffer 대신 heap 메모리 사용
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
            # Iceberg Hadoop catalog
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{catalog}",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog}.warehouse",
                    self._iceberg_cfg.warehouse)
            # 멀티스레드 환경에서 Spark 잡을 FIFO 대신 균등하게 처리
            .config("spark.scheduler.mode", "FAIR")
            # Windows에서 JVM 종료 시 temp jar 삭제 실패 WARN/ERROR 억제 (무해한 Windows 한정 현상)
            # Iceberg 테이블 첫 생성 시 version-hint.text 없음 WARN 억제
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j2.logger.spark_env.name=org.apache.spark.SparkEnv"
                " -Dlog4j2.logger.spark_env.level=OFF"
                " -Dlog4j2.logger.spark_shutdown.name=org.apache.spark.util.ShutdownHookManager"
                " -Dlog4j2.logger.spark_shutdown.level=OFF"
                " -Dlog4j2.logger.iceberg_hadoop_ops.name=org.apache.iceberg.hadoop.HadoopTableOperations"
                " -Dlog4j2.logger.iceberg_hadoop_ops.level=ERROR",
            )
        )
        self._session = builder.getOrCreate()
        self._session.sparkContext.setLogLevel(self._spark_cfg.log_level)

        log.info("SparkSession 생성 완료  (version=%s)", self._session.version)
        return self._session

    @property
    def session(self) -> SparkSession:
        if self._session is None:
            raise RuntimeError("SparkSession이 아직 생성되지 않았습니다. build()를 먼저 호출하세요.")
        return self._session

    def stop(self) -> None:
        if self._session:
            log.info("SparkSession 종료")
            self._session.stop()
            self._session = None

    # context manager
    def __enter__(self) -> SparkSession:
        return self.build()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            log.exception("SparkSession 블록 내 예외 발생: %s", exc_val)
        self.stop()
