"""
Tournament leaderboard stream job.

Reads `tourn-events` from Kafka, explodes each event into one row per player
(white, black) with their score contribution (1 / 0.5 / 0), then runs a
stateful sum aggregation grouped by (tournament_id, player_id). Each output
batch is dual-sinked: Postgres via psycopg2 UPSERT and Cassandra via the
spark-cassandra-connector (which natively upserts on PK collision).
"""

import os
from typing import Iterable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
)


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "tourn-events")
CHECKPOINT = os.environ.get("CHECKPOINT_LOCATION", "/tmp/checkpoints/tournament_leaderboard")

POSTGRES_USER = os.environ.get("POSTGRES_USER", "chess")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "chess123")

WATERMARK_DELAY = "1 hour"


TOURN_EVENT_SCHEMA = StructType([
    StructField("tournament_id", StringType(),  False),
    StructField("game_id",       StringType(),  True),
    StructField("played_at",     LongType(),    True),
    StructField("white_player",  StringType(),  True),
    StructField("black_player",  StringType(),  True),
    StructField("white_elo",     IntegerType(), True),
    StructField("black_elo",     IntegerType(), True),
    StructField("result",        StringType(),  True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("tournament_leaderboard")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def explode_to_player_rows(parsed):
    """Project each tourn-event into two rows (one per player) with score deltas."""
    base_ts = (F.col("played_at") / F.lit(1000)).cast("timestamp")

    white = parsed.select(
        F.col("tournament_id"),
        F.col("white_player").alias("player_id"),
        base_ts.alias("event_time"),
        F.when(F.col("result") == "1-0", 1.0)
         .when(F.col("result") == "1/2-1/2", 0.5)
         .otherwise(0.0).alias("score"),
        F.when(F.col("result") == "1-0", 1).otherwise(0).alias("is_win"),
        F.when(F.col("result") == "1/2-1/2", 1).otherwise(0).alias("is_draw"),
        F.when(F.col("result") == "0-1", 1).otherwise(0).alias("is_loss"),
    )

    black = parsed.select(
        F.col("tournament_id"),
        F.col("black_player").alias("player_id"),
        base_ts.alias("event_time"),
        F.when(F.col("result") == "0-1", 1.0)
         .when(F.col("result") == "1/2-1/2", 0.5)
         .otherwise(0.0).alias("score"),
        F.when(F.col("result") == "0-1", 1).otherwise(0).alias("is_win"),
        F.when(F.col("result") == "1/2-1/2", 1).otherwise(0).alias("is_draw"),
        F.when(F.col("result") == "1-0", 1).otherwise(0).alias("is_loss"),
    )

    return (
        white.unionByName(black)
             .where("player_id IS NOT NULL AND tournament_id IS NOT NULL")
    )


def upsert_postgres(rows: Iterable) -> None:
    """Run on the executor: upsert one Spark partition into Postgres."""
    import psycopg2
    from psycopg2.extras import execute_values

    batch = [
        (
            r["tournament_id"], r["player_id"], float(r["points"]),
            int(r["games_played"]), int(r["wins"]), int(r["draws"]), int(r["losses"]),
            r["last_updated"],
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
                INSERT INTO tournament_leaderboard
                    (tournament_id, player_id, points, games_played,
                     wins, draws, losses, last_updated)
                VALUES %s
                ON CONFLICT (tournament_id, player_id) DO UPDATE SET
                    points       = EXCLUDED.points,
                    games_played = EXCLUDED.games_played,
                    wins         = EXCLUDED.wins,
                    draws        = EXCLUDED.draws,
                    losses       = EXCLUDED.losses,
                    last_updated = EXCLUDED.last_updated
                """,
                batch,
                page_size=500,
            )
    finally:
        conn.close()


def write_batch(df, epoch_id: int) -> None:
    if df.rdd.isEmpty():
        return

    df.persist()
    try:
        # Cassandra: append against PK = native upsert.
        (df.write
           .format("org.apache.spark.sql.cassandra")
           .options(keyspace="chess", table="tournament_standings")
           .mode("append")
           .save())

        # Postgres: explicit ON CONFLICT upsert via psycopg2.
        df.foreachPartition(upsert_postgres)
    finally:
        df.unpersist()


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
        raw.select(F.from_json(F.col("value").cast("string"), TOURN_EVENT_SCHEMA).alias("e"))
           .select("e.*")
    )

    player_rows = explode_to_player_rows(parsed)

    standings = (
        player_rows
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(F.col("tournament_id"), F.col("player_id"))
        .agg(
            F.sum("score").alias("points"),
            F.count(F.lit(1)).alias("games_played"),
            F.sum("is_win").alias("wins"),
            F.sum("is_draw").alias("draws"),
            F.sum("is_loss").alias("losses"),
            F.max("event_time").alias("last_updated"),
        )
    )

    query = (
        standings.writeStream
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
