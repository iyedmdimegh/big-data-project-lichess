"""
Live activity stream job.

Consumes the `game-moves` topic (populated by the Lichess TV live producer)
and runs two streaming queries off the same parsed DataFrame:

    Query A — 1-minute tumbling window with 2-min watermark.
              Aggregates moves_count and distinct active_games per minute.
              Sink: Postgres `live_activity` (upsert on window_start).

    Query B — groupBy(game_id) with 10-min watermark.
              Tracks per-game running move count, first_seen, last_seen.
              Sink: Postgres `game_lengths` (upsert on game_id).

Both queries share the same Kafka source so we only consume the topic once.
"""

import os
from typing import Iterable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
)


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "game-moves")
CHECKPOINT_A = os.environ.get("CHECKPOINT_A", "/tmp/checkpoints/live_activity_throughput")
CHECKPOINT_B = os.environ.get("CHECKPOINT_B", "/tmp/checkpoints/live_activity_lengths")

POSTGRES_USER = os.environ.get("POSTGRES_USER", "chess")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "chess123")


MOVE_EVENT_SCHEMA = StructType([
    StructField("event_type",  StringType(), True),
    StructField("game_id",     StringType(), True),
    StructField("ingested_at", LongType(),   False),   # epoch ms
    StructField("fen",         StringType(), True),
    StructField("last_move",   StringType(), True),
    StructField("wc",          LongType(),   True),
    StructField("bc",          LongType(),   True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("live_activity")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def upsert_activity(rows: Iterable) -> None:
    """Upsert per-minute throughput rows into Postgres `live_activity`."""
    import psycopg2
    from psycopg2.extras import execute_values

    batch = [
        (r["window_start"], r["window_end"], int(r["moves_count"]), int(r["active_games"]))
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
                INSERT INTO live_activity (window_start, window_end, moves_count, active_games)
                VALUES %s
                ON CONFLICT (window_start) DO UPDATE SET
                    window_end   = EXCLUDED.window_end,
                    moves_count  = EXCLUDED.moves_count,
                    active_games = EXCLUDED.active_games
                """,
                batch,
                page_size=500,
            )
    finally:
        conn.close()


def upsert_lengths(rows: Iterable) -> None:
    """Upsert per-game running move counts into Postgres `game_lengths`."""
    import psycopg2
    from psycopg2.extras import execute_values

    batch = [
        (r["game_id"], int(r["move_count"]), r["first_seen"], r["last_seen"])
        for r in rows if r["game_id"] is not None
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
                INSERT INTO game_lengths (game_id, move_count, first_seen, last_seen)
                VALUES %s
                ON CONFLICT (game_id) DO UPDATE SET
                    move_count = EXCLUDED.move_count,
                    first_seen = LEAST(game_lengths.first_seen, EXCLUDED.first_seen),
                    last_seen  = GREATEST(game_lengths.last_seen, EXCLUDED.last_seen)
                """,
                batch,
                page_size=500,
            )
    finally:
        conn.close()


def write_activity(df, epoch_id: int) -> None:
    if df.rdd.isEmpty():
        return
    df.foreachPartition(upsert_activity)


def write_lengths(df, epoch_id: int) -> None:
    if df.rdd.isEmpty():
        return
    df.foreachPartition(upsert_lengths)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), MOVE_EVENT_SCHEMA).alias("m"))
           .select("m.*")
           .withColumn("event_time", (F.col("ingested_at") / F.lit(1000)).cast("timestamp"))
    )

    # Query A — 1-min tumbling throughput.
    throughput = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(F.window(F.col("event_time"), "1 minute").alias("w"))
        .agg(
            F.count(F.lit(1)).alias("moves_count"),
            F.approx_count_distinct(F.col("game_id")).alias("active_games"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("moves_count"),
            F.col("active_games"),
        )
    )

    q_activity = (
        throughput.writeStream
        .outputMode("update")
        .foreachBatch(write_activity)
        .option("checkpointLocation", CHECKPOINT_A)
        .trigger(processingTime="30 seconds")
        .queryName("live_activity_throughput")
        .start()
    )

    # Query B — per-game running move count.
    per_game = (
        parsed
        .withWatermark("event_time", "10 minutes")
        .groupBy(F.col("game_id"))
        .agg(
            F.count(F.lit(1)).alias("move_count"),
            F.min(F.col("event_time")).alias("first_seen"),
            F.max(F.col("event_time")).alias("last_seen"),
        )
    )

    q_lengths = (
        per_game.writeStream
        .outputMode("update")
        .foreachBatch(write_lengths)
        .option("checkpointLocation", CHECKPOINT_B)
        .trigger(processingTime="30 seconds")
        .queryName("live_activity_lengths")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
