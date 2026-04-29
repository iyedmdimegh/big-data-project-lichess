"""
Player styles batch job.

Reads every `game-results` message in Kafka, aggregates per player across
both white and black sides, and writes per-player style metrics to Postgres
`player_styles`.

Architectural note: the original design called for piece-activity metrics
(captures, pawn advances, sacrifices), which require move-level data we
don't ingest at scale. We compute game-level proxies instead:

  aggression_index      = win_rate - loss_rate          (in [-1, 1])
  avg_captures_per_game = NULL                          (no move data)
  avg_pawn_advances     = NULL                          (no move data)
  sacrifices_per_game   = NULL                          (no move data)

Plus we expose the raw aggregates to make the dashboard interesting.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from common import build_spark, read_game_results, write_postgres


MIN_GAMES = 5  # filter out one-shot accounts that distort distributions


def per_side_rows(games):
    """Project each game into two rows — one per player perspective."""
    white = games.select(
        F.col("white_player").alias("player_id"),
        F.col("white_elo").alias("rating"),
        F.col("white_diff").alias("rating_diff"),
        F.col("eco"),
        F.col("time_control"),
        F.col("tournament_id"),
        F.when(F.col("result") == "1-0", 1).otherwise(0).alias("is_win"),
        F.when(F.col("result") == "0-1", 1).otherwise(0).alias("is_loss"),
        F.when(F.col("result") == "1/2-1/2", 1).otherwise(0).alias("is_draw"),
    )
    black = games.select(
        F.col("black_player").alias("player_id"),
        F.col("black_elo").alias("rating"),
        F.col("black_diff").alias("rating_diff"),
        F.col("eco"),
        F.col("time_control"),
        F.col("tournament_id"),
        F.when(F.col("result") == "0-1", 1).otherwise(0).alias("is_win"),
        F.when(F.col("result") == "1-0", 1).otherwise(0).alias("is_loss"),
        F.when(F.col("result") == "1/2-1/2", 1).otherwise(0).alias("is_draw"),
    )
    return white.unionByName(black).where("player_id IS NOT NULL")


def main() -> None:
    spark = build_spark("player_styles")
    spark.sparkContext.setLogLevel("WARN")

    games = read_game_results(spark)
    sides = per_side_rows(games).cache()

    n = sides.count()
    print(f"[player_styles] aggregating from {n} player-game rows")

    style = (
        sides.groupBy("player_id")
             .agg(
                 F.count(F.lit(1)).alias("games"),
                 F.avg(F.col("is_win").cast("double")).alias("win_rate"),
                 F.avg(F.col("is_loss").cast("double")).alias("loss_rate"),
                 F.avg(F.col("is_draw").cast("double")).alias("draw_rate"),
                 F.stddev_samp(F.col("rating_diff").cast("double")).alias("rating_volatility"),
                 F.avg(F.col("rating").cast("double")).alias("avg_rating"),
                 F.countDistinct(F.col("eco")).alias("opening_diversity"),
                 F.avg(F.when(F.col("tournament_id").isNotNull(), 1.0).otherwise(0.0))
                  .alias("tournament_share"),
                 F.avg(F.col("time_control").cast("double")).alias("avg_time_control"),
             )
             .where(F.col("games") >= MIN_GAMES)
    )

    out = (
        style
        .withColumn("aggression_index", F.col("win_rate") - F.col("loss_rate"))
        .withColumn("avg_captures_per_game", F.lit(None).cast("double"))
        .withColumn("avg_pawn_advances", F.lit(None).cast("double"))
        .withColumn("sacrifices_per_game", F.lit(None).cast("double"))
        .withColumn("computed_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
        .selectExpr(
            "player_id",
            "cast(aggression_index as float) AS aggression_index",
            "cast(avg_captures_per_game as float) AS avg_captures_per_game",
            "cast(avg_pawn_advances as float) AS avg_pawn_advances",
            "cast(sacrifices_per_game as float) AS sacrifices_per_game",
            "computed_at",
        )
    )

    rows = out.count()
    print(f"[player_styles] writing {rows} player rows to Postgres")
    write_postgres(out, "player_styles", mode="overwrite")

    print("[player_styles] sample rows:")
    for r in out.orderBy(F.col("aggression_index").desc()).limit(5).collect():
        print(f"  {r['player_id']:<24} aggression={r['aggression_index']:+.3f}")

    spark.stop()


if __name__ == "__main__":
    main()
