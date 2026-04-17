from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml


def _load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


# ── DB 설정 기반 클래스 ────────────────────────────────────────────


class BaseDBConfig:
    """
    MySQL / PostgreSQL 설정이 공통으로 구현해야 하는 인터페이스.

    - jdbc_url              : JDBC 연결 URL (property)
    - jdbc_driver           : JDBC 드라이버 클래스 이름 (property)
    - session_init_statement: Spark JDBC 커넥션 열릴 때 실행할 SQL (property, 기본 "")
    - fqn(schema, table)    : SQL에서 사용할 풀 테이블 이름 (DB 방언에 맞는 따옴표)
    - column_ref(col)       : 컬럼 이름을 DB 방언에 맞게 따옴표로 감싸기
    """

    @property
    def jdbc_url(self) -> str:
        raise NotImplementedError

    @property
    def jdbc_driver(self) -> str:
        raise NotImplementedError

    @property
    def session_init_statement(self) -> str:
        return ""

    def fqn(self, schema: str, table: str) -> str:
        raise NotImplementedError

    def column_ref(self, col: str) -> str:
        raise NotImplementedError


# ── MySQL ─────────────────────────────────────────────────────────


@dataclass
class MySQLConfig(BaseDBConfig):
    host: str = "localhost"
    port: str = "3306"
    user: str = "root"
    password: str = ""
    jdbc_jar_path: str = "drivers/mysql-connector-j-9.2.0.jar"
    _jdbc_driver: str = "com.mysql.cj.jdbc.Driver"

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:mysql://{self.host}:{self.port}/"
            "?useSSL=false&serverTimezone=UTC&allowMultiQueries=true"
        )

    @property
    def jdbc_driver(self) -> str:
        return self._jdbc_driver

    @property
    def session_init_statement(self) -> str:
        return "START TRANSACTION WITH CONSISTENT SNAPSHOT"

    def fqn(self, schema: str, table: str) -> str:
        return f"`{schema}`.`{table}`"

    def column_ref(self, col: str) -> str:
        return f"`{col}`"



# ── PostgreSQL ────────────────────────────────────────────────────


@dataclass
class PostgreSQLConfig(BaseDBConfig):
    """
    PostgreSQL 연결 설정.

    - database  : 접속할 PostgreSQL 데이터베이스 이름
                  (MySQL의 스키마 역할은 list_databases()가 pg 스키마 목록을 반환하는 방식으로 매핑)
    - jdbc_jar_path: PostgreSQL JDBC 드라이버 JAR 경로
                     (예: drivers/postgresql-42.7.4.jar)
    """

    host: str = "localhost"
    port: str = "5432"
    user: str = "postgres"
    password: str = ""
    database: str = "postgres"
    jdbc_jar_path: str = "drivers/postgresql-42.7.4.jar"
    _jdbc_driver: str = "org.postgresql.Driver"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_driver(self) -> str:
        return self._jdbc_driver

    @property
    def session_init_statement(self) -> str:
        # PostgreSQL JDBC는 별도 세션 초기화 불필요
        return ""

    def fqn(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'

    def column_ref(self, col: str) -> str:
        return f'"{col}"'



# ── 기타 설정 ─────────────────────────────────────────────────────


@dataclass
class LoggingConfig:
    level: int = logging.INFO

    @classmethod
    def from_yaml(cls, path: str = "pipeline.yaml") -> LoggingConfig:
        s = _load_yaml(path).get("logging", {})
        level_str = str(s.get("level", "INFO")).upper()
        level = getattr(logging, level_str, logging.INFO)
        return cls(level=level)


@dataclass
class MinIOConfig:
    endpoint: str = "http://localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "lakehouse"
    region: str = "us-east-1"
    ssl_enabled: bool = False

    def path(self, database: str, table: str) -> str:
        return f"s3a://{self.bucket}/{database}/{table}/"

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

    @classmethod
    def from_yaml(cls, path: str = "pipeline.yaml") -> PipelineConfig:
        s = _load_yaml(path).get("pipeline", {})
        return cls(
            num_threads=s.get("num_threads", cls.num_threads),
            chunk_size=s.get("chunk_size", cls.chunk_size),
            large_table_threshold=s.get("large_table_threshold", cls.large_table_threshold),
            large_table_batch_size=s.get("large_table_batch_size", cls.large_table_batch_size),
            large_table_workers=s.get("large_table_workers", cls.large_table_workers),
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
        )
