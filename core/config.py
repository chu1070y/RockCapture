from __future__ import annotations

from functools import lru_cache
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@lru_cache(maxsize=None)
def _load_yaml(path: str) -> dict:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return data or {}


# ── DB 설정 기반 클래스 ────────────────────────────────────────────


class BaseDBConfig(ABC):
    """MySQL / PostgreSQL 설정 공통 인터페이스."""

    @property
    @abstractmethod
    def jdbc_url(self) -> str: ...

    @property
    @abstractmethod
    def jdbc_driver(self) -> str: ...

    @property
    def session_init_statement(self) -> str:
        return ""

    @abstractmethod
    def fqn(self, schema: str, table: str) -> str: ...

    @abstractmethod
    def column_ref(self, col: str) -> str: ...

    @abstractmethod
    def hash_mod_expr(self, col: str, total: int, bucket: int) -> str:
        """hash(col) % total = bucket 형태의 WHERE 절 반환."""
        ...


# ── MySQL ─────────────────────────────────────────────────────────


@dataclass
class MySQLConfig(BaseDBConfig):
    host: str = "localhost"
    port: str = "3306"
    user: str = "root"
    password: str = ""
    jdbc_jar_path: str = "drivers/mysql-connector-j-9.2.0.jar"

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:mysql://{self.host}:{self.port}/"
            "?useSSL=false&serverTimezone=UTC&allowMultiQueries=true"
        )

    @property
    def jdbc_driver(self) -> str:
        return "com.mysql.cj.jdbc.Driver"

    @property
    def session_init_statement(self) -> str:
        return "START TRANSACTION WITH CONSISTENT SNAPSHOT"

    def fqn(self, schema: str, table: str) -> str:
        return f"`{schema}`.`{table}`"

    def column_ref(self, col: str) -> str:
        return f"`{col}`"

    def hash_mod_expr(self, col: str, total: int, bucket: int) -> str:
        # CRC32는 UNSIGNED 반환이므로 ABS 불필요
        return f"MOD(CRC32(`{col}`), {total}) = {bucket}"


# ── PostgreSQL ────────────────────────────────────────────────────


@dataclass
class PostgreSQLConfig(BaseDBConfig):
    host: str = "localhost"
    port: str = "5432"
    user: str = "postgres"
    password: str = ""
    database: str = "postgres"
    jdbc_jar_path: str = "drivers/postgresql-42.7.9.jar"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_driver(self) -> str:
        return "org.postgresql.Driver"

    @property
    def session_init_statement(self) -> str:
        # 각 Spark JDBC 커넥션의 격리 수준을 REPEATABLE READ로 설정.
        # MySQL의 "START TRANSACTION WITH CONSISTENT SNAPSHOT"에 대응.
        return "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ"

    def fqn(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'

    def column_ref(self, col: str) -> str:
        return f'"{col}"'

    def hash_mod_expr(self, col: str, total: int, bucket: int) -> str:
        # HASHTEXT는 음수를 반환할 수 있으므로 ABS 처리
        return f'MOD(ABS(HASHTEXT("{col}")), {total}) = {bucket}'


# ── 기타 설정 ─────────────────────────────────────────────────────


@dataclass
class LoggingConfig:
    level: int = logging.INFO

    @classmethod
    def from_yaml(cls, path: str = "pipeline.yaml") -> LoggingConfig:
        s = _load_yaml(path).get("logging", {})
        level_str = str(s.get("level", "INFO")).upper()
        return cls(level=getattr(logging, level_str, logging.INFO))


@dataclass
class MinIOConfig:
    endpoint: str = "http://localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "lakehouse"
    region: str = "us-east-1"
    ssl_enabled: bool = False

    @classmethod
    def from_yaml(cls, path: str = "pipeline.yaml") -> MinIOConfig:
        s = _load_yaml(path).get("minio", {})
        return cls(
            endpoint=s.get("endpoint", cls.endpoint),
            access_key=s.get("access_key", cls.access_key),
            secret_key=s.get("secret_key", cls.secret_key),
            bucket=s.get("bucket", cls.bucket),
            region=s.get("region", cls.region),
            ssl_enabled=s.get("ssl_enabled", cls.ssl_enabled),
        )


@dataclass
class IcebergConfig:
    catalog_name: str = "lakehouse"
    warehouse: str = "s3a://lakehouse/iceberg-warehouse"

    @classmethod
    def from_yaml(cls, path: str = "pipeline.yaml") -> IcebergConfig:
        s = _load_yaml(path).get("iceberg", {})
        return cls(
            catalog_name=s.get("catalog_name", cls.catalog_name),
            warehouse=s.get("warehouse", cls.warehouse),
        )


@dataclass
class PipelineConfig:
    num_threads: int = 4
    chunk_size: int = 100_000
    large_table_threshold: int = 1_000_000
    large_table_batch_size: int = 1_000_000
    large_table_workers: int = 3
    stats_max_age_seconds: int = 86_400

    @classmethod
    def from_yaml(cls, path: str = "pipeline.yaml") -> PipelineConfig:
        s = _load_yaml(path).get("pipeline", {})
        return cls(
            num_threads=s.get("num_threads", cls.num_threads),
            chunk_size=s.get("chunk_size", cls.chunk_size),
            large_table_threshold=s.get("large_table_threshold", cls.large_table_threshold),
            large_table_batch_size=s.get("large_table_batch_size", cls.large_table_batch_size),
            large_table_workers=s.get("large_table_workers", cls.large_table_workers),
            stats_max_age_seconds=s.get("stats_max_age_seconds", cls.stats_max_age_seconds),
        )


@dataclass
class SparkConfig:
    app_name: str = "LakeHouse"
    log_level: str = "WARN"
    driver_memory: str = "4g"
    extra_packages: list[str] = field(default_factory=lambda: [
        "org.apache.hadoop:hadoop-aws:3.4.1",
        "com.amazonaws:aws-java-sdk-bundle:1.12.720",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
    ])

    @classmethod
    def from_yaml(cls, path: str = "pipeline.yaml") -> SparkConfig:
        s = _load_yaml(path).get("spark", {})
        return cls(
            app_name=s.get("app_name", cls.app_name),
            log_level=s.get("log_level", cls.log_level),
            driver_memory=s.get("driver_memory", cls.driver_memory),
            extra_packages=s.get("extra_packages", cls().extra_packages),
        )
