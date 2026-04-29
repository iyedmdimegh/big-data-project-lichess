"""
K-Means opening clustering (batch).

Reads every `game-results` message in Kafka, aggregates per-ECO features,
standardizes them, runs Spark MLlib KMeans (k=5), and writes cluster
assignments to Postgres `opening_clusters`.

Cluster labels are assigned post-hoc by ranking centroids on a normalized
'aggression score' (high white_win_rate × high game_count → 'sharp/popular').
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow `from common import ...` when submitted as a standalone script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F

from common import build_spark, read_game_results, write_postgres


K = 5
FEATURE_COLS = [
    "game_count",
    "white_win_rate",
    "black_win_rate",
    "draw_rate",
    "avg_white_elo",
    "avg_black_elo",
    "avg_time_control",
]


def build_eco_features(games):
    return (
        games
        .where("eco IS NOT NULL AND eco <> '?'")
        .groupBy("eco")
        .agg(
            F.count(F.lit(1)).alias("game_count"),
            F.first(F.col("opening"), ignorenulls=True).alias("opening_name"),
            F.avg(F.when(F.col("result") == "1-0", 1.0).otherwise(0.0)).alias("white_win_rate"),
            F.avg(F.when(F.col("result") == "0-1", 1.0).otherwise(0.0)).alias("black_win_rate"),
            F.avg(F.when(F.col("result") == "1/2-1/2", 1.0).otherwise(0.0)).alias("draw_rate"),
            F.avg(F.col("white_elo").cast("double")).alias("avg_white_elo"),
            F.avg(F.col("black_elo").cast("double")).alias("avg_black_elo"),
            F.avg(F.col("time_control").cast("double")).alias("avg_time_control"),
        )
        .where("game_count >= 5")  # drop noise — single-game ECOs corrupt clustering
        .na.fill(0.0)
    )


def label_clusters(centers, scaler_means, scaler_stds):
    """Assign human-readable labels to cluster centroids.

    The K-Means centers are in scaled space; un-scale them, then rank by a
    composite 'aggression × popularity' score.
    """
    raw_centers = []
    for c in centers:
        unscaled = [float(c[i]) * scaler_stds[i] + scaler_means[i] for i in range(len(FEATURE_COLS))]
        raw_centers.append(dict(zip(FEATURE_COLS, unscaled)))

    # Rank by white_win_rate (proxy for "sharp aggressive" openings)
    ranked = sorted(range(len(raw_centers)),
                    key=lambda i: raw_centers[i]["white_win_rate"],
                    reverse=True)
    label_pool = [
        "Sharp Tactical",
        "Solid Mainline",
        "Drawish Positional",
        "Black-Friendly",
        "Offbeat / Surprise",
    ]
    labels = {cluster_id: label_pool[rank] for rank, cluster_id in enumerate(ranked)}

    aggression = {}
    for cluster_id, c in enumerate(raw_centers):
        # 0..1 score: tilt toward white_win_rate, penalty for high draw_rate
        aggression[cluster_id] = max(0.0, min(1.0,
            c["white_win_rate"] - 0.5 * c["draw_rate"] + 0.5))

    return labels, aggression


def main() -> None:
    spark = build_spark("kmeans_openings")
    spark.sparkContext.setLogLevel("WARN")

    games = read_game_results(spark)
    eco_feats = build_eco_features(games).cache()

    n = eco_feats.count()
    print(f"[kmeans_openings] aggregated {n} ECO codes")
    if n < K:
        print(f"[kmeans_openings] only {n} ECOs — need at least {K} for K-Means. Skipping.")
        spark.stop()
        return

    # Capture pre-scale means/stds for un-scaling cluster centers later.
    stats = eco_feats.select(
        *[F.avg(F.col(c)).alias(f"mean_{c}") for c in FEATURE_COLS],
        *[F.stddev_samp(F.col(c)).alias(f"std_{c}") for c in FEATURE_COLS],
    ).first()
    scaler_means = [stats[f"mean_{c}"] or 0.0 for c in FEATURE_COLS]
    scaler_stds  = [(stats[f"std_{c}"] or 1.0) or 1.0 for c in FEATURE_COLS]

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withMean=True, withStd=True)

    assembled = assembler.transform(eco_feats)
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)

    kmeans = KMeans(k=K, seed=42, featuresCol="features", predictionCol="cluster_id")
    model = kmeans.fit(scaled)
    clustered = model.transform(scaled)

    labels, aggression = label_clusters(model.clusterCenters(), scaler_means, scaler_stds)

    label_udf = F.udf(lambda cid: labels.get(int(cid), "Unlabeled"))
    aggro_udf = F.udf(lambda cid: float(aggression.get(int(cid), 0.0)))

    out = (
        clustered
        .withColumn("cluster_label", label_udf(F.col("cluster_id")))
        .withColumn("aggression_score", aggro_udf(F.col("cluster_id")))
        .withColumn("updated_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
        .selectExpr(
            "eco AS eco_code",
            "cast(cluster_id as int) AS cluster_id",
            "cluster_label",
            "cast(aggression_score as float) AS aggression_score",
            "updated_at",
        )
    )

    print(f"[kmeans_openings] writing {out.count()} cluster assignments to Postgres")
    write_postgres(out, "opening_clusters", mode="overwrite")

    # Sample log for verification
    print("[kmeans_openings] cluster centers (un-scaled):")
    for cid, label in labels.items():
        print(f"  cluster {cid} = {label}")
    print("[kmeans_openings] sample assignments:")
    for r in out.orderBy(F.rand()).limit(10).collect():
        print(f"  {r['eco_code']} -> {r['cluster_label']} (aggr {r['aggression_score']:.2f})")

    spark.stop()


if __name__ == "__main__":
    main()
