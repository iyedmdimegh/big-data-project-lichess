"""
ELO Tracker stream job.

Reads `game-results` from Kafka, explodes each event into one row per player
(white, black) with (player_id, recorded_at, elo, delta), and dual-sinks the
result to Cassandra (chess.elo_history) and Postgres (elo_history).

Stateless by design: Lichess PGN already contains WhiteRatingDiff /
BlackRatingDiff, so the delta is in the message payload. No state store needed.
"""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
)


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "game-results")
CHECKPOINT = os.environ.get("CHECKPOINT_LOCATION", "/tmp/checkpoints/elo_tracker")

CASSANDRA_KEYSPACE = "chess"
CASSANDRA_TABLE = "elo_history"

POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/chessdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "chess")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "chess123")
POSTGRES_TABLE = "elo_history"


GAME_RESULT_SCHEMA = StructType([
    StructField("game_id",      StringType(),  False),
    StructField("played_at",    LongType(),    False),
    StructField("white_player", StringType(),  False),
    StructField("black_player", StringType(),  False),
    StructField("white_elo",    IntegerType(), True),
    StructField("black_elo",    IntegerType(), True),
    StructField("white_diff",   IntegerType(), True),
    StructField("black_diff",   IntegerType(), True),
    StructField("result",       StringType(),  True),
    StructField("eco",          StringType(),  True),
    StructField("time_control", IntegerType(), True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("elo_tracker")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def to_elo_rows(parsed):
    """Project a parsed game-results stream into one row per player."""
    base_ts = (F.col("played_at") / F.lit(1000)).cast("timestamp")

    white = parsed.select(
        F.col("white_player").alias("player_id"),
        base_ts.alias("recorded_at"),
        F.col("white_elo").alias("elo"),
        F.coalesce(F.col("white_diff"), F.lit(0)).alias("delta"),
    )
    black = parsed.select(
        F.col("black_player").alias("player_id"),
        base_ts.alias("recorded_at"),
        F.col("black_elo").alias("elo"),
        F.coalesce(F.col("black_diff"), F.lit(0)).alias("delta"),
    )
    return (
        white.unionByName(black)
             .where("player_id IS NOT NULL AND elo IS NOT NULL")
    )


def write_batch(df, epoch_id: int) -> None:
    if df.rdd.isEmpty():
        return

    df.persist()
    try:
        (df.write
           .format("org.apache.spark.sql.cassandra")
           .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE)
           .mode("append")
           .save())

        (df.write
           .format("jdbc")
           .option("url", POSTGRES_URL)
           .option("dbtable", POSTGRES_TABLE)
           .option("user", POSTGRES_USER)
           .option("password", POSTGRES_PASSWORD)
           .option("driver", "org.postgresql.Driver")
           .mode("append")
           .save())
    finally:
        df.unpersist()


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
        raw.select(F.from_json(F.col("value").cast("string"), GAME_RESULT_SCHEMA).alias("g"))
           .select("g.*")
    )

    elo = to_elo_rows(parsed)

    query = (
        elo.writeStream
           .foreachBatch(write_batch)
           .option("checkpointLocation", CHECKPOINT)
           .trigger(processingTime="10 seconds")
           .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
