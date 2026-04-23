import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from core.config import SparkConfig


def main() -> None:
    ivy_dir = Path(os.environ.get("SPARK_JARS_IVY", "/app/.ivy2")).resolve()
    bundled_dir = Path(os.environ.get("SPARK_BUNDLED_JARS_DIR", "/app/spark-jars")).resolve()
    ivy_dir.mkdir(parents=True, exist_ok=True)
    bundled_dir.mkdir(parents=True, exist_ok=True)

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

    jars_dir = ivy_dir / "jars"
    if not jars_dir.exists():
        raise FileNotFoundError(f"Resolved Spark jars directory was not found: {jars_dir}")

    copied = 0
    for jar_path in sorted(jars_dir.glob("*.jar")):
        shutil.copy2(jar_path, bundled_dir / jar_path.name)
        copied += 1

    if copied == 0:
        raise RuntimeError("No Spark package jars were copied into the bundled jars directory.")


if __name__ == "__main__":
    main()
