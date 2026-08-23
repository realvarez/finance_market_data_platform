import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


def create_spark_connection(name: str) -> SparkSession | None:
    try:
        # Versions matched to the spark:4.0.2 image runtime (Hadoop 3.4.1).
        # NOTE: under `spark-submit`, spark.jars.packages set here is IGNORED —
        # pass the same list via --packages (see candle-builder in docker-compose.yml).
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2",
            "org.apache.hadoop:hadoop-aws:3.4.1",
            "com.amazonaws:aws-java-sdk-bundle:1.12.780",
        ]

        spark = (
            SparkSession.builder.appName(name)
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark connection created successfully")
        return spark
    except Exception:
        logger.exception("Failed to create Spark session")
        return None


def connect_to_kafka(spark: SparkSession, topic: str, starting_offsets: str = "latest"):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def write_to_kafka(df, topic: str, checkpoint: str):
    return (
        df.selectExpr("to_json(struct(*)) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )
