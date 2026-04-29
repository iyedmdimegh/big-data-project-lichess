"""
ALS opening recommender (batch).

Reads every `game-results` message in Kafka, builds a (player, ECO) implicit-
feedback matrix where the value is play count, trains Spark MLlib ALS, then
emits the top-K recommended ECOs per player to Postgres `als_vectors`.

The `factors` JSONB column stores the recommendations directly (not raw user
factors) so the dashboard can render them without further processing.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from common import build_spark, read_game_results, write_postgres


MIN_GAMES_PER_PLAYER = 5
MIN_GAMES_PER_ECO = 5
TOP_K_RECS = 10


def build_interactions(games):
    """Build (player_id, eco, count) implicit-feedback rows from game-results."""
    white = games.select(F.col("white_player").alias("player_id"), F.col("eco"))
    black = games.select(F.col("black_player").alias("player_id"), F.col("eco"))
    return (
        white.unionByName(black)
             .where("player_id IS NOT NULL AND eco IS NOT NULL AND eco <> '?'")
             .groupBy("player_id", "eco")
             .agg(F.count(F.lit(1)).alias("plays"))
    )


def main() -> None:
    spark = build_spark("als_recommender")
    spark.sparkContext.setLogLevel("WARN")

    games = read_game_results(spark)
    interactions = build_interactions(games).cache()

    # Drop sparse rows so ALS converges on meaningful patterns.
    player_counts = interactions.groupBy("player_id").agg(F.sum("plays").alias("total"))
    eco_counts = interactions.groupBy("eco").agg(F.sum("plays").alias("total"))
    active_players = player_counts.where(F.col("total") >= MIN_GAMES_PER_PLAYER).select("player_id")
    common_ecos = eco_counts.where(F.col("total") >= MIN_GAMES_PER_ECO).select("eco")

    filtered = (
        interactions
        .join(active_players, "player_id")
        .join(common_ecos, "eco")
    )
    n_inter = filtered.count()
    n_players = filtered.select("player_id").distinct().count()
    n_ecos = filtered.select("eco").distinct().count()
    print(f"[als] {n_inter} interactions, {n_players} players, {n_ecos} ECOs")

    if n_players < 2 or n_ecos < 2:
        print("[als] not enough data — exiting")
        spark.stop()
        return

    player_idx = StringIndexer(inputCol="player_id", outputCol="player_idx",
                               handleInvalid="skip").fit(filtered)
    eco_idx = StringIndexer(inputCol="eco", outputCol="eco_idx",
                            handleInvalid="skip").fit(filtered)

    indexed = eco_idx.transform(player_idx.transform(filtered))

    als = ALS(
        userCol="player_idx",
        itemCol="eco_idx",
        ratingCol="plays",
        rank=20,
        maxIter=10,
        regParam=0.1,
        implicitPrefs=True,
        coldStartStrategy="drop",
        seed=42,
    )
    model = als.fit(indexed)

    # Top-K recommendations per player (in indexed space).
    recs = model.recommendForAllUsers(TOP_K_RECS)  # player_idx, recommendations[(eco_idx, rating)]

    # Build mapping back from idx → original strings.
    eco_lookup = {float(i): label for i, label in enumerate(eco_idx.labels)}
    player_lookup = {float(i): label for i, label in enumerate(player_idx.labels)}
    eco_lookup_b = spark.sparkContext.broadcast(eco_lookup)
    player_lookup_b = spark.sparkContext.broadcast(player_lookup)

    def to_json(player_idx_val, rec_list):
        items = []
        for r in rec_list:
            items.append({
                "eco": eco_lookup_b.value.get(float(r["eco_idx"]), "?"),
                "score": float(r["rating"]),
            })
        return json.dumps(items, ensure_ascii=False)

    def player_name(idx_val):
        return player_lookup_b.value.get(float(idx_val), "?")

    to_json_udf = F.udf(to_json)
    name_udf = F.udf(player_name)

    out = (
        recs
        .select(
            name_udf(F.col("player_idx")).alias("player_id"),
            to_json_udf(F.col("player_idx"), F.col("recommendations")).alias("factors"),
        )
        .withColumn("updated_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
    )

    print(f"[als] writing {out.count()} player recommendation vectors")

    # JDBC can't write JSONB directly — use overwrite then ALTER COLUMN as needed.
    # Simplest path: write to a staging table, then INSERT ... SELECT with cast.
    write_postgres(out, "als_vectors_stage", mode="overwrite")
    spark.stop()

    # Promote staging → real table with JSONB cast.
    import psycopg2
    conn = psycopg2.connect(host="postgres", port=5432, dbname="chessdb",
                            user="chess", password="chess123")
    try:
        with conn, conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE als_vectors")
            cur.execute("""
                INSERT INTO als_vectors (player_id, factors, updated_at)
                SELECT player_id, factors::jsonb, updated_at
                FROM als_vectors_stage
            """)
            cur.execute("DROP TABLE als_vectors_stage")
        print("[als] promoted staging to als_vectors with JSONB cast")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
