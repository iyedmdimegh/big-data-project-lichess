"""
Live featured-game stream job.

Consumes the `player-events` topic (populated by the Lichess TV live producer)
and writes each `game_featured` event to Postgres `featured_players`. Upserts
on `game_id` PK — if Lichess re-features the same game, the latest snapshot
wins.

Stateless: one Kafka event → one Postgres row.
"""

import os
from typing import Iterable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
)


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "player-events")
CHECKPOINT = os.environ.get("CHECKPOINT_LOCATION", "/tmp/checkpoints/live_featured")

POSTGRES_USER = os.environ.get("POSTGRES_USER", "chess")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "chess123")


SIDE_SCHEMA = StructType([
    StructField("name",   StringType(), True),
    StructField("title",  StringType(), True),
    StructField("rating", LongType(),   True),
])

PLAYER_EVENT_SCHEMA = StructType([
    StructField("event_type",  StringType(), True),
    StructField("game_id",     StringType(), True),
    StructField("ingested_at", LongType(),   False),
    StructField("orientation", StringType(), True),
    StructField("white",       SIDE_SCHEMA,  True),
    StructField("black",       SIDE_SCHEMA,  True),
    StructField("fen",         StringType(), True),
    StructField("wc",          LongType(),   True),
    StructField("bc",          LongType(),   True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("live_featured")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def upsert_featured(rows: Iterable) -> None:
    """Upsert featured-game snapshots into Postgres."""
    import psycopg2
    from psycopg2.extras import execute_values

    batch = [
        (
            r["game_id"],
            r["featured_at"],
            r["white_player"], r["white_title"], r["white_rating"],
            r["black_player"], r["black_title"], r["black_rating"],
        )
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
                INSERT INTO featured_players
                    (game_id, featured_at, white_player, white_title, white_rating,
                     black_player, black_title, black_rating)
                VALUES %s
                ON CONFLICT (game_id) DO UPDATE SET
                    featured_at  = EXCLUDED.featured_at,
                    white_player = EXCLUDED.white_player,
                    white_title  = EXCLUDED.white_title,
                    white_rating = EXCLUDED.white_rating,
                    black_player = EXCLUDED.black_player,
                    black_title  = EXCLUDED.black_title,
                    black_rating = EXCLUDED.black_rating
                """,
                batch,
                page_size=100,
            )
    finally:
        conn.close()


def write_batch(df, epoch_id: int) -> None:
    if df.rdd.isEmpty():
        return
    df.foreachPartition(upsert_featured)


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
        raw.select(F.from_json(F.col("value").cast("string"), PLAYER_EVENT_SCHEMA).alias("e"))
           .select("e.*")
           .where("event_type = 'game_featured'")
           .select(
               F.col("game_id"),
               (F.col("ingested_at") / F.lit(1000)).cast("timestamp").alias("featured_at"),
               F.col("white.name").alias("white_player"),
               F.col("white.title").alias("white_title"),
               F.col("white.rating").cast("int").alias("white_rating"),
               F.col("black.name").alias("black_player"),
               F.col("black.title").alias("black_title"),
               F.col("black.rating").cast("int").alias("black_rating"),
           )
    )

    query = (
        parsed.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
