# Chess Pipeline — Demo Runbook

End-to-end guide for presenting the chess analytics big data pipeline.

---

## TL;DR — The pitch

> "Lichess publishes 100M+ chess games every month. We built a 5-layer big data pipeline that ingests both their historical PGN dumps (batch, ~1 GB/month compressed) and their live TV stream (real-time NDJSON), processes everything through Kafka, runs three Spark Structured Streaming jobs and three Spark MLlib batch jobs, persists results to a polyglot store (Cassandra + PostgreSQL), and visualizes in Grafana — all on Docker Compose."

Architecture map (per the project diagram):

```
data sources    →   Lichess PGN dumps (batch)  +  Lichess TV API (live NDJSON)
ingestion       →   2 Python producers (batch_pgn + stream)
message bus     →   Apache Kafka (5 topics)
processing      →   3 Spark Streaming jobs  +  3 Spark MLlib batch jobs
storage         →   Cassandra (time-series) + PostgreSQL (relational/analytics) + Elasticsearch (search)
visualization   →   4 Grafana dashboards
```

The deferred pieces (Stockfish/bot detection, move NLP) are documented as future work.

---

## 1. Pre-demo checklist

Run these *before* the audience walks in. If anything fails, see "Recovery" at the bottom.

```bash
# 1.1  All 14 containers healthy
docker compose ps

# 1.2  Three streaming jobs alive
docker exec big-data-project-lichess-spark-master-1 sh -c \
  "ps -ef | grep -v grep | grep -E 'elo_tracker.py|opening_trends.py|leaderboard.py' | wc -l"
# expect: 9   (3 wrapper sh + 3 java SparkSubmit + 3 python3 drivers)

# 1.3  Kafka topics exist
docker exec big-data-project-lichess-kafka-1 kafka-topics \
  --bootstrap-server kafka:9092 --list
# expect: game-results, game-moves, player-events, tourn-events

# 1.4  Postgres has data
docker exec big-data-project-lichess-postgres-1 psql -U chess -d chessdb -c "
  SELECT 'elo_history' AS t, COUNT(*) FROM elo_history UNION ALL
  SELECT 'opening_trends', COUNT(*) FROM opening_trends UNION ALL
  SELECT 'tournament_leaderboard', COUNT(*) FROM tournament_leaderboard UNION ALL
  SELECT 'opening_clusters', COUNT(*) FROM opening_clusters UNION ALL
  SELECT 'player_styles', COUNT(*) FROM player_styles UNION ALL
  SELECT 'als_vectors', COUNT(*) FROM als_vectors;"

# 1.5  Cassandra has data
docker exec big-data-project-lichess-cassandra-1 cqlsh -e "
  SELECT count(*) FROM chess.elo_history;
  SELECT count(*) FROM chess.tournament_standings;"
```

Expected ballpark numbers (after our test runs):

| Table | Rows |
|---|---|
| `elo_history` (Postgres) | ~20 000 |
| `opening_trends` | ~2 500 |
| `tournament_leaderboard` | ~600 |
| `opening_clusters` | 287 |
| `player_styles` | ~3 800 |
| `als_vectors` | ~3 800 |

---

## 2. The five URLs to keep open in browser tabs

| What | URL | Login |
|---|---|---|
| Kafka UI | http://localhost:8080 | none |
| Spark Master UI | http://localhost:8090 | none |
| Grafana | http://localhost:3000 | `admin` / `admin123` |
| Elasticsearch | http://localhost:9200 | none |
| Kibana (search UI) | http://localhost:5601 | none |

Open all five in tabs before the demo so you can switch fast.

---

## 3. The demo flow (recommended order, ~15-20 min)

### Step 1 — Show the architecture (1 min)

Bring up the architecture diagram from the project brief. State the 5 layers and the dual ingestion (batch + stream). Mention "this is what we built end-to-end."

### Step 2 — Show the infrastructure (2 min)

Switch to a terminal:

```bash
docker compose ps --format "table {{.Service}}\t{{.Status}}"
```

Talk through it: "14 services on a single Docker Compose. Zookeeper + Kafka are the message bus. Spark master + 2 workers are the cluster. Cassandra + Postgres + Elasticsearch are polyglot storage. Grafana + Metabase + Kibana are the visualization layer."

### Step 3 — Show ingestion & Kafka (3 min)

Open **Kafka UI** at http://localhost:8080 → click **chess-cluster** → **Topics**.

Talk track:
- `game-results` — "from the batch PGN producer; one event per finished game with ELO, ECO, time control, etc. ~30 000 messages here from our test runs."
- `game-moves` — "from the live Lichess TV stream; per-move events with the FEN position and clocks."
- `player-events` — "also from the live producer; emitted whenever a new game becomes 'featured' on Lichess TV."
- `tourn-events` — "derived from `game-results` by parsing the PGN `Event` header for tournament URLs."

Click into `game-results` → "Messages" → show the JSON payload of one message.

**Optional live demo**: open another terminal and run

```powershell
.\.venv\Scripts\python.exe -m ingestion.stream_producer.main `
  --bootstrap localhost:29092 --max-reconnects -1
```

Switch back to Kafka UI → `game-moves` → refresh: messages arrive in real time. After ~30s, kill the producer with Ctrl+C.

### Step 4 — Show stream processing in Spark UI (2 min)

Open **Spark Master UI** at http://localhost:8090. Show the three running applications:

- `elo_tracker` — "stateless Structured Streaming; explodes each game into 2 player rows and dual-sinks to both Cassandra and Postgres."
- `opening_trends` — "5-minute tumbling-window aggregation with a 5-minute watermark. Counts games per ECO."
- `tournament_leaderboard` — "stateful aggregation grouped by `(tournament_id, player_id)`. Uses `outputMode('update')` so only changed standings are flushed each trigger."

Mention the 4-core constraint: "We have 4 cores, 3 streaming jobs, so each is capped at 1 core in the submit scripts. Batch jobs use all 4 — but we pause streaming first."

### Step 5 — Walk the four Grafana dashboards (5-7 min)

Open **Grafana** at http://localhost:3000 (`admin` / `admin123`) → **Dashboards** → **Chess Pipeline** folder.

#### 5a. ELO Tracker

- Time range is preset to **2016-01-31 21:30 to 23:00 UTC** — that's the actual played time of our PGN sample.
- Pick any player from the **Player** dropdown — say `microcommega`.
- The time-series shows that player's ELO over their session. Stat panels show total ELO updates and distinct players tracked.
- *Talk track*: "This is what 'streaming end-to-end' looks like. Each new game emits two events into Kafka; Spark picks them up within 10 seconds; both Cassandra and Postgres get written; Grafana auto-refreshes every 10s. If we ran the producer right now, this chart would extend to the right within seconds."

#### 5b. Opening Trends

- **Stacked bar chart** of the most-played openings per 5-minute window.
- The "Top openings overall" table shows the dominant ECOs over the time range.
- *Talk track*: "This demonstrates Spark's *stateful* streaming with watermarks. Each window tracks the count for each ECO; we use `outputMode('update')` so only changed counts are upserted. The top opening was `A00 Hungarian Opening` with 536 games — that's actually mostly 1.b3 / oddities in our 2016 sample."

#### 5c. Tournament Leaderboard

- Pick a tournament from the dropdown — most have 50+ players.
- Table panel shows top-20 standings with rank, player, points, games, W/D/L.
- The histogram at the bottom shows the distribution of points (right-skewed — most players have low scores, only a few cluster at the top).
- *Talk track*: "This is the stateful aggregation pattern: per-`(tournament_id, player_id)`, we sum points (1 for win, 0.5 for draw, 0 for loss). Cassandra natively upserts on PK collision; for Postgres we use `INSERT … ON CONFLICT DO UPDATE` via psycopg2. Note `Fanatist` with 11/11 — perfect score, exactly the kind of pattern bot-detection would flag."

#### 5d. Batch Insights

- **Pie chart**: 5 K-Means cluster sizes (Drawish 159, Sharp Tactical 54, Offbeat 49, Black-Friendly 15, Solid Mainline 10).
- **Sample ECOs per cluster**: 6 random ECO codes per cluster.
- **Aggression histogram**: 3782 players' aggression index (`win_rate − loss_rate`), centered near 0.
- **Top-15 most aggressive players**.
- **Recommendations for $player**: pick a player → ALS top-10 ECO recommendations with cluster annotation.

*Talk track*: "Three batch ML jobs. K-Means clusters the 287 ECO codes by features like white-win-rate and average ELO. Player styles aggregates per-player metrics across 3782 players. ALS does collaborative filtering — `(player, ECO, play count)` matrix, rank-20, implicit-feedback model — and produces personalized opening recommendations. Each player's recommendations are unique."

### Step 6 — Inspect raw storage (3 min)

This shows you didn't just build dashboards — the underlying polyglot store is real.

#### Cassandra (time-series, write-heavy)

```bash
# Open cqlsh
docker exec -it big-data-project-lichess-cassandra-1 cqlsh

# Inside cqlsh:
USE chess;
DESCRIBE TABLES;

-- Time-ordered ELO history for one player (CLUSTERING ORDER BY recorded_at DESC)
SELECT player_id, recorded_at, elo, delta
FROM elo_history
WHERE player_id = 'microcommega'
LIMIT 10;

-- One tournament's standings (sorted client-side)
SELECT player_id, points, games_played, wins, draws, losses
FROM tournament_standings
WHERE tournament_id = '<paste-id-from-grafana>';

EXIT;
```

*Talk track*: "Cassandra is the architectural source of truth for time-series — high write throughput, predictable latency. The PK design `(player_id, recorded_at) WITH CLUSTERING ORDER BY recorded_at DESC` means 'latest ELO for player X' is a single sequential read."

#### PostgreSQL (relational, indexed for analytics)

```bash
docker exec -it big-data-project-lichess-postgres-1 psql -U chess -d chessdb

-- Inside psql:
\dt              -- list tables
\d elo_history   -- describe a table

-- Top 5 most-played ECOs across the whole dataset
SELECT eco, opening_name, SUM(game_count) AS total
FROM opening_trends
GROUP BY eco, opening_name
ORDER BY total DESC LIMIT 5;

-- Sample ALS recommendation (JSONB)
SELECT player_id, jsonb_pretty(factors)
FROM als_vectors
LIMIT 1;

\q
```

*Talk track*: "Postgres carries the analytical, indexed views — Grafana datasource, JSONB for ALS factors, full SQL for joins. Same data also lives in Cassandra; we dual-sink because Grafana's Cassandra plugin is unsigned and dual-sink keeps Postgres as a fast presentation cache."

#### Elasticsearch (full-text search, scaffolded)

```bash
curl http://localhost:9200/_cluster/health
curl http://localhost:9200/_cat/indices
```

*Talk track*: "Elasticsearch index mappings for `chess_openings` are defined; we'd populate it from the K-Means batch job in a future iteration to power the React opening explorer."

#### Kafka (the message bus)

```bash
# End offsets per topic
docker exec big-data-project-lichess-kafka-1 \
  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic game-results

# Tail one topic live (Ctrl+C to exit)
docker exec -it big-data-project-lichess-kafka-1 \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic game-moves --from-beginning --max-messages 3
```

### Step 7 — Run a batch job live (optional, 2 min)

Only if you have time. This is dramatic because it visibly recomputes ML output.

```bash
# Pause streaming jobs first to free cores (otherwise batch crawls)
docker exec big-data-project-lichess-spark-master-1 pkill -9 -f opening_trends.py
docker exec big-data-project-lichess-spark-master-1 pkill -9 -f elo_tracker.py
docker exec big-data-project-lichess-spark-master-1 pkill -9 -f leaderboard.py

# Run K-Means re-clustering
bash batch_processing/kmeans_openings/submit.sh

# Verify
docker exec big-data-project-lichess-postgres-1 psql -U chess -d chessdb -c \
  "SELECT cluster_label, COUNT(*) FROM opening_clusters GROUP BY cluster_label;"

# Restart streaming jobs
bash stream_processing/elo_tracker/submit.sh
bash stream_processing/opening_trends/submit.sh
bash stream_processing/tournament_leaderboard/submit.sh
```

*Talk track*: "The batch jobs read `game-results` from Kafka in batch mode (`endingOffsets=latest`), so they treat Kafka as both stream and batch source. Same data, different read pattern. K-Means with k=5 takes ~30 seconds end-to-end on this dataset."

---

## 4. End-to-end fresh test (if you want to demo from a clean slate)

Skip this for the actual presentation — it takes ~3 min. Useful for rehearsals.

```bash
# 4.1  Clear everything
docker exec big-data-project-lichess-cassandra-1 cqlsh -e \
  "TRUNCATE chess.elo_history; TRUNCATE chess.tournament_standings;"
docker exec big-data-project-lichess-postgres-1 psql -U chess -d chessdb -c \
  "TRUNCATE elo_history; TRUNCATE opening_trends; TRUNCATE tournament_leaderboard;
   TRUNCATE opening_clusters; TRUNCATE player_styles; TRUNCATE als_vectors;"
docker exec big-data-project-lichess-spark-master-1 sh -c "rm -rf /tmp/checkpoints/*"

# 4.2  Confirm streaming jobs are running (restart if needed)
bash stream_processing/elo_tracker/submit.sh
bash stream_processing/opening_trends/submit.sh
bash stream_processing/tournament_leaderboard/submit.sh

# 4.3  Run the batch PGN producer for a fresh feed
.\.venv\Scripts\python.exe -m ingestion.batch_pgn_producer.main `
  --pgn data/lichess_db_standard_rated_2016-02.pgn.zst `
  --bootstrap localhost:29092 --topic game-results --tourn-topic tourn-events `
  --rate 800 --max-games 10000

# 4.4  Wait 30-60 s for streams to flush, then verify
docker exec big-data-project-lichess-postgres-1 psql -U chess -d chessdb -c \
  "SELECT COUNT(*) FROM elo_history;
   SELECT COUNT(*) FROM opening_trends;
   SELECT COUNT(*) FROM tournament_leaderboard;"

# 4.5  Pause streams, run all batch jobs (re-fills opening_clusters / player_styles / als_vectors)
docker exec big-data-project-lichess-spark-master-1 pkill -9 -f opening_trends.py
docker exec big-data-project-lichess-spark-master-1 pkill -9 -f elo_tracker.py
docker exec big-data-project-lichess-spark-master-1 pkill -9 -f leaderboard.py
bash batch_processing/kmeans_openings/submit.sh
bash batch_processing/player_styles/submit.sh
bash batch_processing/als_recommender/submit.sh

# 4.6  Restart streams
bash stream_processing/elo_tracker/submit.sh
bash stream_processing/opening_trends/submit.sh
bash stream_processing/tournament_leaderboard/submit.sh
```

---

## 5. Anticipated Q&A

### "Why dual-sink to Cassandra and Postgres?"

> "Architecturally Cassandra is the source of truth — write-heavy, time-series. Postgres is a presentation cache for Grafana, which has a built-in Postgres datasource but only a community/unsigned Cassandra plugin. Costs us one extra `df.write` per batch — minimal — and keeps the architecture clean."

### "Why Kafka if everything's in one Docker host?"

> "Kafka is the architectural backbone, not just a message queue. It decouples producers from consumers — we have one batch and one stream producer feeding the same `game-results` topic, and three streaming jobs plus three batch jobs reading from it independently. Adding a fourth consumer is zero-coordination. Plus replay: with `startingOffsets=earliest`, our batch jobs reprocess the full topic on demand."

### "Why is the data from 2016?"

> "Our local sample is the Feb 2016 standard-rated dump (~867 MB compressed, ~3-5 GB uncompressed, ~15-30 M games). The pipeline isn't time-locked — point the producer at any monthly dump from `database.lichess.org` and you're streaming current data. The live producer hits the real Lichess TV API right now."

### "Three streaming jobs on 4 cores — how?"

> "Each is capped at `spark.cores.max=1` in the submit script. They share 3 of the 4 cores, leaving 1 free for misc. Batch jobs need more cores, so we pause streaming when running batch — explicit in our docs. Production would scale workers."

### "Where's bot detection?"

> "Deferred. It needs the engine match rate from Stockfish UCI analysis — out of scope for this iteration. The architectural place is reserved (`bot_scores` table in Cassandra exists, the `Bot detection` box is in the diagram). Adding it is well-scoped: one additional Spark Streaming job consuming `game-moves`, plus a Stockfish container."

### "Where's Move NLP?"

> "Also deferred. The architecture targets game annotations / commentary text — the standard rated dump doesn't include those. The Lichess annotated dataset on Kaggle would be the source; we'd ingest separately."

### "Why ALS specifically?"

> "Spark MLlib has a production-grade implementation, scales horizontally (factor matrices are partitioned), supports implicit feedback (we use play counts, not ratings — there's no '4 stars for the French Defense'). For this dataset, rank=20 with 10 iterations converges in seconds."

### "Why K=5 for K-Means?"

> "Empirically — 5 produced the cleanest separation between sharp / drawish / offbeat / mainline / Black-friendly. We did not sweep K with silhouette score; for an academic deliverable, the labels we generated are post-hoc and human-readable."

### "Tournament IDs from the PGN — how?"

> "The Lichess PGN `Event` header contains a tournament URL, e.g. `Rated Blitz tournament https://lichess.org/tournament/abc123`. A regex extracts the slug. About 17% of games in our sample are tournament games."

### "Per-second ELO collisions in the data?"

> "Real and noted in the plan. PGN `UTCTime` is per-second; same player can finish two bullet games in the same second. Cassandra silently upserts on `(player_id, recorded_at)` PK; Postgres rejects under JDBC INSERT. We solved by giving Postgres `elo_history` a `BIGSERIAL id` PK with the player+time as a non-unique index — append-only event log semantics."

### "Why no Airflow?"

> "Academic scope. Manual `spark-submit` for batch is simpler and matches the brief. Adding Airflow is a one-page change to docker-compose and one DAG per batch job."

---

## 6. The deferred / future-work pitch

When asked "what's next" or "what would you build given more time":

1. **Stockfish container + bot-detection stream job** — engine match rate per player, real cheat detection. Needs the existing `game-moves` topic (already populated by live producer).
2. **K-Means feedback loop** — re-publish cluster IDs to the `openings` Kafka topic so streaming jobs can enrich live games with cluster labels (this is the dashed feedback arrow in the architecture diagram).
3. **React + FastAPI front-end** — opening explorer where you input your move history and get ALS recommendations + cluster info. Read path: Postgres for ALS/clusters, Elasticsearch for opening name fuzzy search, Cassandra for live ELO trend.
4. **Metabase analytical dashboards** — the architecture distinguishes Grafana (live operator view) from Metabase (analyst view). Same Postgres datasource; metabase is up at port 3003.
5. **Airflow for batch scheduling** — replace `spark-submit` with proper DAGs.
6. **Move NLP** — ingest Lichess annotated games dataset separately, run Spark NLP for sentiment / theme tagging on commentary.

---

## 7. Recovery (if something breaks during the demo)

### A streaming job died

```bash
# Check
docker exec big-data-project-lichess-spark-master-1 sh -c \
  "ps -ef | grep -v grep | grep -E 'elo_tracker.py|opening_trends.py|leaderboard.py'"

# Restart whichever is missing
bash stream_processing/elo_tracker/submit.sh
bash stream_processing/opening_trends/submit.sh
bash stream_processing/tournament_leaderboard/submit.sh
```

### Grafana shows "no data"

1. Check time range — most dashboards default to **2016-01-31 21:30 – 23:00 UTC** (the PGN data date). If you accidentally clicked "now-7d", you'll see nothing.
2. Hard-refresh the browser tab.
3. Verify data: `docker exec big-data-project-lichess-postgres-1 psql -U chess -d chessdb -c "SELECT COUNT(*) FROM <table>;"`

### "All my pie chart slices are the same value"

The piechart `reduceOptions` must be `{"calcs":["lastNotNull"], "fields":"", "values":true}` — `values:true` is what makes each row a slice instead of reducing all rows to one.

### Kafka topic is missing after a restart

```bash
docker exec big-data-project-lichess-kafka-1 kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic game-results --partitions 3 --replication-factor 1
docker exec big-data-project-lichess-kafka-1 kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic game-moves --partitions 6 --replication-factor 1
docker exec big-data-project-lichess-kafka-1 kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic player-events --partitions 3 --replication-factor 1
docker exec big-data-project-lichess-kafka-1 kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic tourn-events --partitions 3 --replication-factor 1
```

### Postgres `init.sql` schema didn't apply

```bash
docker exec -i big-data-project-lichess-postgres-1 psql -U chess -d chessdb < storage/postgres/init.sql
```

### Need to nuke and restart cleanly

```bash
docker compose down
docker compose up -d
# Wait ~60 s, then verify with section 1.
# Re-create topics if missing (above), then submit streaming jobs.
```

---

## 8. One-liner cheat sheet

| Goal | Command |
|---|---|
| Status of everything | `docker compose ps` |
| Are streams running? | `docker exec big-data-project-lichess-spark-master-1 sh -c "ps -ef \| grep -v grep \| grep -E 'tracker.py\|trends.py\|leaderboard.py' \| wc -l"` (expect 9) |
| Kafka topic offsets | `docker exec big-data-project-lichess-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic game-results` |
| Postgres row counts | `docker exec big-data-project-lichess-postgres-1 psql -U chess -d chessdb -c "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"` |
| Cassandra row counts | `docker exec big-data-project-lichess-cassandra-1 cqlsh -e "SELECT count(*) FROM chess.elo_history; SELECT count(*) FROM chess.tournament_standings;"` |
| Open cqlsh | `docker exec -it big-data-project-lichess-cassandra-1 cqlsh` |
| Open psql | `docker exec -it big-data-project-lichess-postgres-1 psql -U chess -d chessdb` |
| Tail a Kafka topic | `docker exec -it big-data-project-lichess-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic game-moves --from-beginning --max-messages 5` |
| Run live producer | `.\.venv\Scripts\python.exe -m ingestion.stream_producer.main --bootstrap localhost:29092 --max-reconnects -1` |
| Run batch producer | `.\.venv\Scripts\python.exe -m ingestion.batch_pgn_producer.main --pgn data/lichess_db_standard_rated_2016-02.pgn.zst --bootstrap localhost:29092 --rate 800 --max-games 10000` |

---

## 9. Architectural map of the codebase

```
big-data-project-lichess/
├── docker-compose.yml          # 14 services, custom chess-net
├── .env                        # all ports / credentials
├── data/                       # Lichess PGN sample (867 MB compressed)
├── ingestion/
│   ├── batch_pgn_producer/     # PGN → game-results + tourn-events
│   └── stream_producer/        # Lichess TV API → player-events + game-moves
├── stream_processing/
│   ├── elo_tracker/            # stateless, dual-sink
│   ├── opening_trends/         # 5-min tumbling windows
│   └── tournament_leaderboard/ # stateful, output-mode update
├── batch_processing/
│   ├── common.py               # shared Kafka batch reader + Spark builder
│   ├── kmeans_openings/        # MLlib K-Means (k=5)
│   ├── player_styles/          # per-player aggregation
│   └── als_recommender/        # MLlib ALS, implicit feedback, JSONB output
├── storage/
│   ├── cassandra/init.cql      # 4 tables: game_moves, elo_history, bot_scores, tournament_standings
│   ├── postgres/init.sql       # 8 tables incl. elo_history, opening_trends, tournament_leaderboard, opening_clusters, player_styles, als_vectors
│   └── elasticsearch/mappings.json
├── visualization/
│   └── grafana/provisioning/
│       ├── datasources/postgres.yaml
│       └── dashboards/         # 4 dashboards: chess-overview, elo-tracker, opening-trends, tournament-leaderboard, batch-insights
└── spark/Dockerfile            # bitnami spark + numpy + Kafka/Cassandra/Postgres connector JARs + spark user fix
```

Key implementation details to mention if asked:

- The Bitnami Spark image runs as UID 1001 with no `/etc/passwd` entry → Hadoop's JAAS NPEs on `getpwuid`. Fixed by appending a `spark:x:1001` line in the Dockerfile.
- Spark `spark.jars.ivy` defaults to `~/.ivy2` which evaluates to `?/.ivy2` for that user. Set explicitly to `/tmp/.ivy2` in every submit script.
- The Cassandra Spark connector and Postgres JDBC are baked into the Spark image (not `--packages`) so submits don't need network at runtime.
