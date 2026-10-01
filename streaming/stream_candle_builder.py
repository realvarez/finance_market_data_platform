import logging
import os
import sys

from pyspark.sql.functions import (
    col,
    first,
    from_json,
    last,
    window,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ingestion import config
from streaming.spark_utils import connect_to_kafka, create_spark_connection, write_to_kafka

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

TICK_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("symbol", StringType()),
        StructField("timestamp", StringType()),
        StructField("ingestion_timestamp", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("bid", DoubleType()),
        StructField("ask", DoubleType()),
        StructField("source", StringType()),
    ]
)

INTERVALS = {"1m": "1 minute", "5m": "5 minutes"}
WATERMARK_DELAY = os.getenv("SPARK_WATERMARK_DELAY", "10 seconds")
CHECKPOINT_BUCKET = config.MINIO_CHECKPOINT_BUCKET


def build_candles(spark, interval: str, window_duration: str):
    kafka_df = connect_to_kafka(spark, config.TOPIC_TICKS)

    parsed = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), TICK_SCHEMA).alias("tick"))
        .select("tick.*")
        .withColumn("event_time", col("timestamp").cast(TimestampType()))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    candles = (
        parsed.groupBy(col("symbol"), window(col("event_time"), window_duration))
        .agg(
            first("price").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            last("price").alias("close"),
            spark_sum("volume").alias("volume"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("timestamp"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
        )
    )

    output = candles.selectExpr(
        "concat(symbol, date_format(timestamp, 'yyyyMMddHHmmss')) as event_id",
        "symbol",
        f"'{interval}' as interval",
        "cast(timestamp as string) as timestamp",
        "open",
        "high",
        "low",
        "close",
        "coalesce(volume, 0) as volume",
        f"'{config.SOURCE_SPARK}' as source",
        "cast(current_timestamp() as string) as created_at",
    )

    checkpoint = f"s3a://{CHECKPOINT_BUCKET}/checkpoints/candles_{interval}"
    return write_to_kafka(output, config.TOPIC_CANDLES_CALCULATED, checkpoint)


def main():
    spark = create_spark_connection("StreamCandleBuilder")
    if not spark:
        sys.exit(1)

    queries = []
    for interval, duration in INTERVALS.items():
        logger.info("Starting candle builder for interval %s", interval)
        query = build_candles(spark, interval, duration)
        queries.append(query)

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
