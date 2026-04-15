from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml


def _load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


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
class MySQLConfig:
    host: str = "localhost"
    port: str = "3306"
    user: str = "root"
    password: str = ""
    jdbc_jar_path: str = "drivers/mysql-connector-j-9.2.0.jar"
    jdbc_driver: str = "com.mysql.cj.jdbc.Driver"  # MariaDB: org.mariadb.jdbc.Driver

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:mysql://{self.host}:{self.port}/"
            "?useSSL=false&serverTimezone=UTC&allowMultiQueries=true"
        )

    @classmethod
    def from_yaml(cls, path: str = "pipeline.yaml") -> MySQLConfig:
        s = _load_yaml(path).get("mysql", {})
        return cls(
            host=s.get("host", cls.host),
            port=str(s.get("port", cls.port)),
            user=s.get("user", cls.user),
            password=s.get("password", cls.password),
            jdbc_jar_path=s.get("jdbc_jar_path", cls.jdbc_jar_path),
            jdbc_driver=s.get("jdbc_driver", cls.jdbc_driver),
        )


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
    chunk_size: int = 100_000          # JDBC 파티션당 행 수 (num_partitions 계산용)
    large_table_threshold: int = 1_000_000   # 이 행 수 이상이면 대형 테이블로 분류
    large_table_batch_size: int = 1_000_000  # 대형 테이블 PK 범위 배치 크기
    large_table_workers: int = 3             # 대형 테이블 동시 처리 수

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
