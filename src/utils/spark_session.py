import os
from pathlib import Path

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    minio_endpoint = os.getenv("MINIO_ENDPOINT") or "http://minio:9000"
    minio_access_key = os.getenv("MINIO_ROOT_USER") or "minioadmin"
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin123"

    if Path("/opt/airflow").exists():
        ivy_dir = Path("/opt/airflow/.ivy2")
    else:
        ivy_dir = Path.home() / ".ivy2"

    ivy_dir.mkdir(parents=True, exist_ok=True)
    jars_dir = ivy_dir / "jars"
    jars_dir.mkdir(parents=True, exist_ok=True)

    jar_paths = sorted(str(path) for path in jars_dir.glob("*.jar"))

    builder = (
    SparkSession.builder
    .appName(app_name)
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ])
    )
    .config("spark.jars.ivy", str(ivy_dir))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.warehouse.dir", "/opt/sentinelpay/dbt/sentinelpay_dbt/spark-warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    )

    spark = builder.enableHiveSupport().getOrCreate()

    if jar_paths:
        builder = builder.config("spark.jars", ",".join(jar_paths))

    if Path("/opt/airflow").exists():
        builder = (
            builder
            .config("spark.driver.extraClassPath", f"{jars_dir}/*")
            .config("spark.executor.extraClassPath", f"{jars_dir}/*")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark