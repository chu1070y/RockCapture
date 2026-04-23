import os
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from core.config import BaseDBConfig, IcebergConfig, MinIOConfig, SparkConfig
from core.logger import get_logger

# Windows specific Hadoop path defaults.
if sys.platform == "win32":
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")

# Ensure Spark workers use the current Python interpreter.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

log = get_logger(__name__)


class SparkSessionManager:
    """Create and manage a SparkSession."""

    def __init__(
        self,
        spark_cfg: SparkConfig,
        db_cfg: BaseDBConfig,
        minio_cfg: MinIOConfig,
        iceberg_cfg: IcebergConfig,
        temp_dir: str | None = None,
    ):
        self._spark_cfg = spark_cfg
        self._db_cfg = db_cfg
        self._minio_cfg = minio_cfg
        self._iceberg_cfg = iceberg_cfg
        resolved_temp_dir = temp_dir or os.environ.get("SPARK_TEMP_DIR")
        self._temp_dir = Path(resolved_temp_dir).resolve() if resolved_temp_dir else None
        self._session: SparkSession | None = None

    @staticmethod
    def _resolve_jar_paths(paths: list[str]) -> list[str]:
        jar_paths: list[str] = []
        for raw_path in paths:
            jar_path = Path(raw_path).resolve()
            if not jar_path.exists():
                raise FileNotFoundError(f"Spark jar was not found: {jar_path}")
            jar_paths.append(str(jar_path))
        return jar_paths

    def _spark_jars(self) -> list[str]:
        return self._resolve_jar_paths(
            [self._db_cfg.jdbc_jar_path, *self._spark_cfg.extra_jars]
        )

    def _ivy_cache_dir(self) -> str | None:
        ivy_dir = self._spark_cfg.ivy_cache_dir or os.environ.get("SPARK_JARS_IVY")
        if not ivy_dir:
            return None

        ivy_path = Path(ivy_dir).resolve()
        ivy_path.mkdir(parents=True, exist_ok=True)
        return str(ivy_path)

    def _stop_existing_sessions(self) -> None:
        active_session = SparkSession.getActiveSession()
        if active_session is not None:
            log.info("Stopping active SparkSession before creating a new one")
            active_session.stop()

        default_session = getattr(SparkSession, "_instantiatedSession", None)
        if default_session is not None and default_session is not active_session:
            log.info("Stopping default SparkSession before creating a new one")
            default_session.stop()

    def _prepare_temp_dir(self) -> str | None:
        if self._temp_dir is None:
            return None

        self._temp_dir.mkdir(parents=True, exist_ok=True)
        log.info("Using project-local Spark temp dir: %s", self._temp_dir)
        return str(self._temp_dir)

    def _cleanup_temp_dir(self) -> None:
        if self._temp_dir is None or not self._temp_dir.exists():
            return

        for attempt in range(1, 6):
            try:
                shutil.rmtree(self._temp_dir)
                log.info("Removed Spark temp dir: %s", self._temp_dir)
                return
            except OSError as exc:
                if attempt == 5:
                    log.warning("Could not fully remove Spark temp dir %s: %s", self._temp_dir, exc)
                    return
                time.sleep(0.5 * attempt)

    def build(self) -> SparkSession:
        log.info("SparkSession 생성 시작  (app=%s)", self._spark_cfg.app_name)

        spark_jars = self._spark_jars()
        spark_temp_dir = self._prepare_temp_dir()
        ivy_cache_dir = self._ivy_cache_dir()
        log.debug("Spark jars: %s", spark_jars)
        log.debug("extra packages: %s", self._spark_cfg.extra_packages)
        log.debug("MinIO endpoint: %s", self._minio_cfg.endpoint)
        if ivy_cache_dir:
            log.debug("Ivy cache dir: %s", ivy_cache_dir)

        catalog = self._iceberg_cfg.catalog_name
        self._stop_existing_sessions()

        java_options = [
            "-Dlog4j2.logger.spark_env.name=org.apache.spark.SparkEnv",
            "-Dlog4j2.logger.spark_env.level=OFF",
            "-Dlog4j2.logger.spark_shutdown.name=org.apache.spark.util.ShutdownHookManager",
            "-Dlog4j2.logger.spark_shutdown.level=OFF",
            "-Dlog4j2.logger.iceberg_hadoop_ops.name=org.apache.iceberg.hadoop.HadoopTableOperations",
            "-Dlog4j2.logger.iceberg_hadoop_ops.level=ERROR",
            "-Dlog4j2.logger.java_utils.name=org.apache.spark.network.util.JavaUtils",
            "-Dlog4j2.logger.java_utils.level=OFF",
            "-Dlog4j2.logger.spark_utils.name=org.apache.spark.util.Utils",
            "-Dlog4j2.logger.spark_utils.level=OFF",
        ]
        if spark_temp_dir:
            java_options.append(f"-Djava.io.tmpdir={spark_temp_dir}")

        builder = (
            SparkSession.builder
            .appName(self._spark_cfg.app_name)
            .config("spark.driver.memory", self._spark_cfg.driver_memory)
            .config("spark.jars", ",".join(spark_jars))
            .config("spark.hadoop.fs.s3a.endpoint", self._minio_cfg.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self._minio_cfg.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self._minio_cfg.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(self._minio_cfg.ssl_enabled).lower(),
            )
            .config("spark.hadoop.fs.s3a.endpoint.region", self._minio_cfg.region)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog}.warehouse", self._iceberg_cfg.warehouse)
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.driver.extraJavaOptions", " ".join(java_options))
        )
        if self._spark_cfg.extra_packages:
            builder = builder.config(
                "spark.jars.packages",
                ",".join(self._spark_cfg.extra_packages),
            )
        if ivy_cache_dir:
            builder = builder.config("spark.jars.ivy", ivy_cache_dir)
        if spark_temp_dir:
            builder = (
                builder
                .config("spark.local.dir", spark_temp_dir)
                .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={spark_temp_dir}")
            )

        self._session = builder.getOrCreate()
        self._session.sparkContext.setLogLevel(self._spark_cfg.log_level)

        log.info("SparkSession 생성 완료  (version=%s)", self._session.version)
        return self._session

    @property
    def session(self) -> SparkSession:
        if self._session is None:
            raise RuntimeError("SparkSession is not initialized. Call build() first.")
        return self._session

    def stop(self) -> None:
        if self._session:
            log.info("SparkSession 종료")
            self._session.stop()
            self._session = None
        self._cleanup_temp_dir()

    def __enter__(self) -> SparkSession:
        return self.build()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            log.exception("SparkSession block raised an exception: %s", exc_val)
        self.stop()
