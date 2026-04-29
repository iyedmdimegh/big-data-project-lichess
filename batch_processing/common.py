"""Helpers shared across the nightly batch Spark jobs."""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
)


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "game-results")

POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/chessdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "chess")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "chess123")


GAME_RESULT_SCHEMA = StructType([
    StructField("game_id",       StringType(),  False),
    StructField("played_at",     LongType(),    True),
    StructField("white_player",  StringType(),  True),
    StructField("black_player",  StringType(),  True),
    StructField("white_elo",     IntegerType(), True),
    StructField("black_elo",     IntegerType(), True),
    StructField("white_diff",    IntegerType(), True),
    StructField("black_diff",    IntegerType(), True),
    StructField("result",        StringType(),  True),
    StructField("eco",           StringType(),  True),
    StructField("opening",       StringType(),  True),
    StructField("time_control",  IntegerType(), True),
    StructField("tournament_id", StringType(),  True),
])


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_game_results(spark: SparkSession):
    """Batch-read every `game-results` message currently in Kafka."""
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), GAME_RESULT_SCHEMA).alias("g"))
           .select("g.*")
    )


def write_postgres(df, table: str, mode: str = "overwrite") -> None:
    """Bulk JDBC write — used by batch jobs that fully replace target tables."""
    (df.write
       .format("jdbc")
       .option("url", POSTGRES_URL)
       .option("dbtable", table)
       .option("user", POSTGRES_USER)
       .option("password", POSTGRES_PASSWORD)
       .option("driver", "org.postgresql.Driver")
       .mode(mode)
       .save())
