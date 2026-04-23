from pathlib import Path

from pyspark.sql import SparkSession

from core.config import SparkConfig


def main() -> None:
    ivy_dir = Path("tmp/docker-ivy").resolve()
    ivy_dir.mkdir(parents=True, exist_ok=True)

    cfg = SparkConfig()
    builder = (
        SparkSession.builder
        .appName("rockcapture-prefetch")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.ivy", str(ivy_dir))
    )

    if cfg.extra_packages:
        builder = builder.config("spark.jars.packages", ",".join(cfg.extra_packages))

    spark = builder.getOrCreate()
    spark.range(1).count()
    spark.stop()


if __name__ == "__main__":
    main()
