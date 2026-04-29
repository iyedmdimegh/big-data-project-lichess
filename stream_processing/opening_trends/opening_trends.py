"""
Opening trends stream job.

Reads `game-results` from Kafka, applies a 5-minute tumbling window over the
event-time `played_at`, counts games per ECO opening per window, and upserts
the results to Postgres `opening_trends`.

Demonstrates Spark Structured Streaming windowing (the ELO tracker is
stateless; this one is stateful with a watermark).
"""

import os
from typing import Iterable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
)


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "game-results")
CHECKPOINT = os.environ.get("CHECKPOINT_LOCATION", "/tmp/checkpoints/opening_trends")

POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/chessdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "chess")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "chess123")

WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "5 minutes"


GAME_RESULT_SCHEMA = StructType([
    StructField("game_id",      StringType(),  False),
    StructField("played_at",    LongType(),    False),
    StructField("white_player", StringType(),  True),
    StructField("black_player", StringType(),  True),
    StructField("white_elo",    IntegerType(), True),
    StructField("black_elo",    IntegerType(), True),
    StructField("white_diff",   IntegerType(), True),
    StructField("black_diff",   IntegerType(), True),
    StructField("result",       StringType(),  True),
    StructField("eco",          StringType(),  True),
    StructField("opening",      StringType(),  True),
    StructField("time_control", IntegerType(), True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("opening_trends")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def upsert_partition(rows: Iterable) -> None:
    """Run inside executors: upsert one Spark partition into Postgres."""
    import psycopg2  # imported on the executor — installed in the chess-spark image
    from psycopg2.extras import execute_values

    batch = [
        (
            r["window_start"],
            r["window_end"],
            r["eco"],
            r["opening_name"],
            int(r["game_count"]),
        )
        for r in rows
    ]
    if not batch:
        return

    conn = psycopg2.connect(
        host="postgres", port=5432,
        dbname="chessdb", user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO opening_trends
                    (window_start, window_end, eco, opening_name, game_count)
                VALUES %s
                ON CONFLICT (window_start, eco)
                DO UPDATE SET
                    window_end   = EXCLUDED.window_end,
                    opening_name = EXCLUDED.opening_name,
                    game_count   = EXCLUDED.game_count
                """,
                batch,
                page_size=500,
            )
    finally:
        conn.close()


def write_batch(df, epoch_id: int) -> None:
    if df.rdd.isEmpty():
        return
    df.foreachPartition(upsert_partition)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), GAME_RESULT_SCHEMA).alias("g"))
           .select("g.*")
           .where("eco IS NOT NULL AND eco <> '?'")
           .withColumn("event_time", (F.col("played_at") / F.lit(1000)).cast("timestamp"))
    )

    windowed = (
        parsed
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION).alias("w"),
            F.col("eco"),
        )
        .agg(
            F.count(F.lit(1)).alias("game_count"),
            F.first(F.col("opening"), ignorenulls=True).alias("opening_name"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("eco"),
            F.col("opening_name"),
            F.col("game_count"),
        )
    )

    query = (
        windowed.writeStream
        .outputMode("update")  # emit windows whose count changed each trigger
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
