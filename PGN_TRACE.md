# PGN Trace — How One Batch Of Games Becomes Dashboards

This doc follows the lifecycle of a single PGN game through every layer of the
pipeline as built today. Pick any game from the dump; we use a real one from
[data/sample_games.csv](data/sample_games.csv) as the worked example.

If you understand this trace, you understand the whole project end-to-end.

---

## Stage 0 — What a PGN file actually looks like

PGN (Portable Game Notation) is the standard text format for chess games.
Lichess publishes monthly bulk dumps as Zstandard-compressed PGN files at
`database.lichess.org`. Our local sample is
[data/lichess_db_standard_rated_2016-02.pgn.zst](data/lichess_db_standard_rated_2016-02.pgn.zst)
— ~867 MB compressed, ~3-5 GB uncompressed, on the order of 15-30 million games.

A "batch of PGN" is just a stream of these blocks concatenated, one game per
block. Each game has two parts: a header section in `[Tag "Value"]` form, and
a moves section.

### Example game (real, from our sample)

```
[Event "Rated Blitz tournament https://lichess.org/tournament/oSyPykCM"]
[Site "https://lichess.org/OIR2O8JN"]
[Date "2016.01.31"]
[Round "-"]
[White "Ayman22"]
[Black "daamien"]
[Result "1-0"]
[UTCDate "2016.01.31"]
[UTCTime "23:00:02"]
[WhiteElo "1364"]
[BlackElo "1414"]
[WhiteRatingDiff "+11"]
[BlackRatingDiff "-11"]
[ECO "B10"]
[Opening "Caro-Kann Defense: Two Knights Attack"]
[TimeControl "300+0"]
[Termination "Normal"]

1. e4 c6 2. Nf3 d6 3. Nc3 Nf6 4. d4 e5 { [%clk 0:05:00] [%clk 0:05:00] }
5. dxe5 dxe5 6. Bc4 Bg4 7. h3 Bxf3 8. Qxf3 Bb4 9. O-O Bxc3 10. bxc3 Nbd7
... (rest of the moves) ...
22. Qxh7# 1-0
```

**Key fields the pipeline uses:**

| Header | Used by | Why |
|---|---|---|
| `Event` | producer | tournament URL parsed for `tournament_id` |
| `Site` | producer | game URL → `game_id` |
| `White` / `Black` | every job | player identity |
| `WhiteElo` / `BlackElo` | ELO tracker, player styles, ALS | rating |
| `WhiteRatingDiff` / `BlackRatingDiff` | ELO tracker | per-game ELO delta — already pre-computed by Lichess |
| `Result` | tournament leaderboard, opening trends, player styles | 1-0 / 0-1 / 1/2-1/2 |
| `ECO` / `Opening` | opening trends, K-Means, ALS | classification code + name |
| `UTCDate` + `UTCTime` | every job | event time → `played_at` epoch ms |
| `TimeControl` | K-Means | feature for clustering |

`%eval` and `%clk` annotations between moves are also parsed (per-move clock
times and Stockfish evals), but the standard rated dump has no `%eval` data —
only `%clk`. Move-level data isn't currently consumed by any downstream job
(see "What's deferred" at the end).

---

## Stage 1 — Parsing PGN to structured records

**Code:**
[ingestion/batch_pgn_producer/pgn_parser.py](ingestion/batch_pgn_producer/pgn_parser.py)

The parser is a memory-efficient generator: it streams one game at a time
from the `.pgn.zst` file using `zstandard.ZstdDecompressor.stream_reader`,
hands the text block to `python-chess`'s `chess.pgn.read_game`, and yields
a normalized dict per game. Nothing is loaded into memory in bulk.

For our example game, `parse_pgn(...)` yields:

```python
{
    "id": "OIR2O8JN",
    "rated": True,
    "variant": "standard",
    "event": "Rated Blitz tournament https://lichess.org/tournament/oSyPykCM",
    "created_at": 1454281202000,         # epoch ms of UTCDate+UTCTime
    "result": "1-0",
    "winner": "white",
    "termination": "Normal",
    "eco": "B10",
    "opening": "Caro-Kann Defense: Two Knights Attack",
    "time_control": {"initial": 300, "increment": 0},
    "players": {
        "white": {
            "name": "Ayman22",
            "rating": 1364,                # post-game rating
            "rating_diff": 11,             # +11 from this game
            "title": None,
        },
        "black": {
            "name": "daamien",
            "rating": 1414,
            "rating_diff": -11,
            "title": None,
        },
    },
    "moves": "e4 c6 Nf3 d6 Nc3 Nf6 d4 e5 ...",
    "ply_count": 43,
    "evals": None,                          # no engine annotations in this dump
    "clocks": [300, 300, ..., 0],           # per-move clock if %clk present
}
```

This shape stays stable across the whole pipeline. Every downstream job
operates on derivatives of this record.

---

## Stage 2 — Producer projects to Kafka events

**Code:** [ingestion/batch_pgn_producer/main.py](ingestion/batch_pgn_producer/main.py)

The producer iterates `parse_pgn(...)`, calls `to_event()` to project the
game into a flat Kafka payload, and `to_tourn_event()` for tournament games.
A simple token-bucket throttles to `--rate` messages/second so downstream
streams don't drown.

### What gets emitted to `game-results`

```json
{
  "game_id": "OIR2O8JN",
  "played_at": 1454281202000,
  "white_player": "Ayman22",
  "black_player": "daamien",
  "white_elo": 1364,
  "black_elo": 1414,
  "white_diff": 11,
  "black_diff": -11,
  "result": "1-0",
  "eco": "B10",
  "opening": "Caro-Kann Defense: Two Knights Attack",
  "time_control": 300,
  "tournament_id": "oSyPykCM"
}
```

- **Topic:** `game-results` (3 partitions)
- **Key:** `game_id` (UTF-8) — partitions by game so re-runs are idempotent
- **Value:** UTF-8 JSON (above)

### What gets emitted to `tourn-events` (only if `tournament_id` is present)

```json
{
  "tournament_id": "oSyPykCM",
  "game_id": "OIR2O8JN",
  "played_at": 1454281202000,
  "white_player": "Ayman22",
  "black_player": "daamien",
  "white_elo": 1364,
  "black_elo": 1414,
  "result": "1-0"
}
```

- **Topic:** `tourn-events` (3 partitions)
- **Key:** `tournament_id`

About 17% of games in our sample are tournament games (Lichess tournaments
include their slug in the `Event` header — extracted by the regex
`r"lichess.org/tournament/([A-Za-z0-9]+)"`).

### Run command

```powershell
.\.venv\Scripts\python.exe -m ingestion.batch_pgn_producer.main `
  --pgn data/lichess_db_standard_rated_2016-02.pgn.zst `
  --bootstrap localhost:29092 `
  --topic game-results --tourn-topic tourn-events `
  --rate 800 --max-games 10000
```

Producer uses `kafka-python` with `acks=1, linger_ms=20, batch_size=65536` —
fire-and-forget batching for max throughput. Output of a typical run:

```
INFO batch_pgn_producer sent=10000 sent_tourn=1728 skipped=0
```

---

## Stage 3 — Kafka holds the messages

**Compose service:** [docker-compose.yml](docker-compose.yml) (see `kafka:` and
`zookeeper:`)

Kafka is the architectural backbone. Each topic is a durable, partitioned,
append-only log. Once messages land in Kafka, multiple independent consumers
read them at their own pace — that's why we have three streaming jobs and
three batch jobs all reading the same `game-results` topic without any
coordination.

| Topic | Partitions | Producer | Consumers |
|---|---|---|---|
| `game-results` | 3 | batch PGN | elo_tracker, opening_trends + 3 batch jobs |
| `tourn-events` | 3 | batch PGN (only tournament games) | tournament_leaderboard |
| `game-moves` | 6 | live stream | (no consumer yet — visualized in Kafka UI) |
| `player-events` | 3 | live stream | (no consumer yet) |

Inspect from outside:

```bash
docker exec big-data-project-lichess-kafka-1 \
  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic game-results
```

```
game-results:0:8592
game-results:1:8898
game-results:2:8510    # cumulative ~26k messages from prior runs
```

---

## Stage 4 — Stream jobs consume and aggregate

Three Spark Structured Streaming jobs run concurrently, each capped at 1
core (4-core cluster, 3 streams + headroom for batch). All read from Kafka
via the `spark-sql-kafka-0-10` connector baked into the Spark image.

### 4a. ELO Tracker

**Code:**
[stream_processing/elo_tracker/elo_tracker.py](stream_processing/elo_tracker/elo_tracker.py)
**Topic:** `game-results`
**Pattern:** **stateless** — explode each event into 2 rows (white + black), dual-sink

For our example game, the job emits two rows:

| player_id | recorded_at | elo | delta |
|---|---|---|---|
| Ayman22 | 2016-01-31 23:00:02 | 1364 | +11 |
| daamien | 2016-01-31 23:00:02 | 1414 | -11 |

Trigger every 10 seconds. Each batch:

1. Cassandra `chess.elo_history` — append (PK `(player_id, recorded_at)`, native upsert on collision)
2. Postgres `elo_history` — append (BIGSERIAL `id` PK so per-second collisions don't break JDBC)

**Why stateless?** Lichess PGN already includes `WhiteRatingDiff` /
`BlackRatingDiff` — the delta is in the message. No state store needed.

### 4b. Opening Trends

**Code:**
[stream_processing/opening_trends/opening_trends.py](stream_processing/opening_trends/opening_trends.py)
**Topic:** `game-results`
**Pattern:** **stateful, windowed** — 5-min tumbling windows on event time

For our example game (`played_at = 2016-01-31 23:00:02`, `eco = B10`), the job:

1. Falls into the `[2016-01-31 23:00, 23:05)` window
2. Increments the count for `(window=23:00, eco=B10)` by 1
3. Carries forward `opening_name = "Caro-Kann Defense: Two Knights Attack"`
4. Watermark: `event_time - 5 min` (drop very-late events)
5. Output mode `update`: only emit windows whose count changed each trigger

Trigger every 30 seconds. Sink: Postgres `opening_trends` with PK
`(window_start, eco)` and `psycopg2 execute_values + ON CONFLICT DO UPDATE`
(JDBC append doesn't support upsert, hence the explicit psycopg2 path).

After our example game lands, a row exists like:

| window_start | window_end | eco | opening_name | game_count |
|---|---|---|---|---|
| 2016-01-31 23:00 | 2016-01-31 23:05 | B10 | Caro-Kann Defense: Two Knights Attack | 1 |

(plus rows for every other ECO in that 5-min window — typically 100-300
distinct ECOs per window in busy periods.)

### 4c. Tournament Leaderboard

**Code:**
[stream_processing/tournament_leaderboard/leaderboard.py](stream_processing/tournament_leaderboard/leaderboard.py)
**Topic:** `tourn-events`
**Pattern:** **stateful aggregation** — per `(tournament_id, player_id)`

Our example game *is* a tournament game (`tournament_id = oSyPykCM`), so it
also lands here. Job explodes each event into two player rows with a score
contribution:

| tournament_id | player_id | event_time | score | is_win | is_draw | is_loss |
|---|---|---|---|---|---|---|
| oSyPykCM | Ayman22 | 2016-01-31 23:00:02 | 1.0 | 1 | 0 | 0 |
| oSyPykCM | daamien | 2016-01-31 23:00:02 | 0.0 | 0 | 0 | 1 |

Then runs a stateful sum aggregation:

```python
groupBy("tournament_id", "player_id")
  .agg(sum("score") AS points, count(*) AS games_played, ...)
```

Watermark: `1 hour` (tournaments are short-lived, so 1 hour is more than
enough delay tolerance).

Sink: dual — Cassandra `chess.tournament_standings` (append, native upsert)
and Postgres `tournament_leaderboard` (psycopg2 ON CONFLICT). Every batch,
output mode `update` emits only the players whose totals changed.

---

## Stage 5 — Batch ML jobs consume the same topic

Batch jobs read `game-results` *as a batch*, not a stream:

```python
spark.read.format("kafka")
   .option("startingOffsets", "earliest")
   .option("endingOffsets", "latest")
```

This snapshots the entire topic at submit time. Same Kafka, different read
pattern — Kafka serves as both stream and batch source. Run via
`spark-submit` after temporarily pausing the streaming jobs (the 4-core
cluster can't run both heavy batch and 3 streams at once).

### 5a. K-Means Opening Clustering

**Code:**
[batch_processing/kmeans_openings/kmeans_openings.py](batch_processing/kmeans_openings/kmeans_openings.py)

For each ECO across the entire topic, computes feature vector:

| Feature | For our example ECO `B10` |
|---|---|
| game_count | (sum of all `B10` games) |
| white_win_rate | avg of `result == "1-0"` |
| black_win_rate | avg of `result == "0-1"` |
| draw_rate | avg of `result == "1/2-1/2"` |
| avg_white_elo | avg `white_elo` |
| avg_black_elo | avg `black_elo` |
| avg_time_control | avg `time_control` (seconds) |

`StandardScaler` (zero-mean, unit-variance) → MLlib `KMeans(k=5)`. Cluster
labels assigned post-hoc by ranking centroids on `white_win_rate` (proxy for
"sharp / aggressive"). Result row for our example:

| eco_code | cluster_id | cluster_label | aggression_score |
|---|---|---|---|
| B10 | 2 | Solid Mainline | 0.98 |

Sink: Postgres `opening_clusters` (full overwrite each run).

### 5b. Player Styles

**Code:**
[batch_processing/player_styles/player_styles.py](batch_processing/player_styles/player_styles.py)

Per-player aggregation across both colors. For our example game, two
projection rows feed in:

- `(player_id=Ayman22, is_win=1, is_loss=0, is_draw=0, rating=1364, rating_diff=+11, eco=B10, ...)` (white perspective)
- `(player_id=daamien, is_win=0, is_loss=1, is_draw=0, rating=1414, rating_diff=-11, eco=B10, ...)` (black perspective)

Aggregated per player into:

| Field | Meaning |
|---|---|
| games | count of player-side rows |
| win_rate, loss_rate, draw_rate | avg of is_win, is_loss, is_draw |
| rating_volatility | stddev of rating_diff |
| opening_diversity | distinct ECO count |
| tournament_share | fraction with `tournament_id IS NOT NULL` |
| `aggression_index` | `win_rate − loss_rate` ∈ `[-1, +1]` |

Filter: `games >= 5` to drop one-shot accounts. Sink: Postgres
`player_styles` (full overwrite).

> **Honest note:** the architecture's original spec called for
> "captures / pawn advances / sacrifices" as aggression metrics — those need
> move-level data. We don't ingest moves at scale (only `game-moves` from the
> live producer, no batch path). The fields exist in the table but are
> populated as `NULL`; `aggression_index` is the game-level proxy.

### 5c. ALS Recommender

**Code:**
[batch_processing/als_recommender/als_recommender.py](batch_processing/als_recommender/als_recommender.py)

Builds an implicit-feedback matrix `(player_id, eco, plays)` from
`game-results`. For our example game, two rows:

```
(Ayman22, B10, 1)
(daamien, B10, 1)
```

Filters to active players (≥5 games) and common ECOs (≥5 games). Encodes
strings → integer IDs via `StringIndexer`. Trains MLlib `ALS` with
`rank=20, maxIter=10, regParam=0.1, implicitPrefs=True, seed=42`. Then
`model.recommendForAllUsers(10)` produces top-10 ECO recommendations per
player.

For Ayman22, the resulting JSON might look like:

```json
[
  {"eco": "B12", "score": 1.213},
  {"eco": "C00", "score": 1.045},
  {"eco": "B01", "score": 0.972},
  ...
]
```

JDBC writes the JSON as text → staging table → `INSERT … SELECT factors::jsonb`
into the real `als_vectors(player_id, factors JSONB, updated_at)` via
`psycopg2`. (Spark's JDBC writer doesn't support `JSONB` directly.)

---

## Stage 6 — Storage layout

Polyglot persistence. Same logical data lives in different stores tuned for
different access patterns.

### Cassandra (write-heavy time-series, source of truth)

```cql
USE chess;

elo_history (
  player_id text, recorded_at timestamp, elo int, delta int,
  PRIMARY KEY (player_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

tournament_standings (
  tournament_id text, player_id text, points float, games_played int,
  wins int, draws int, losses int, last_updated timestamp,
  PRIMARY KEY (tournament_id, player_id)
);
```

After our example game:

```cql
SELECT * FROM chess.elo_history WHERE player_id = 'Ayman22';
-- 2016-01-31 23:00:02 | 1364 | +11

SELECT * FROM chess.tournament_standings WHERE tournament_id = 'oSyPykCM';
-- ... | Ayman22  | 1.0 | 1 | 1 | 0 | 0 | ...
-- ... | daamien  | 0.0 | 1 | 0 | 0 | 1 | ...
```

### PostgreSQL (relational + analytics, presentation cache)

```sql
elo_history (id BIGSERIAL PK, player_id, recorded_at, elo, delta);
opening_trends (window_start, window_end, eco, opening_name, game_count, PK(window_start, eco));
tournament_leaderboard (tournament_id, player_id, points, games_played, ...,
                        PK(tournament_id, player_id));
opening_clusters (eco_code PK, cluster_id, cluster_label, aggression_score, updated_at);
player_styles (player_id PK, aggression_index, ..., computed_at);
als_vectors (player_id PK, factors JSONB, updated_at);
```

After our example game has worked through everything (and the batch jobs
have run on the full topic):

```sql
SELECT * FROM elo_history WHERE player_id IN ('Ayman22', 'daamien');
-- 2 rows

SELECT * FROM opening_trends WHERE eco = 'B10' AND window_start = '2016-01-31 23:00';
-- 1 row, game_count = (number of B10 games in that window)

SELECT * FROM tournament_leaderboard WHERE tournament_id = 'oSyPykCM' ORDER BY points DESC;
-- multiple rows, our two players among them

SELECT * FROM opening_clusters WHERE eco_code = 'B10';
-- B10 | 2 | "Solid Mainline" | 0.98

SELECT player_id, factors->0->>'eco' AS top_rec
FROM als_vectors WHERE player_id IN ('Ayman22', 'daamien');
-- top-1 personalized recommendation per player
```

### Elasticsearch — scaffolded but not populated

`chess_openings` index mapping is defined in
[storage/elasticsearch/mappings.json](storage/elasticsearch/mappings.json).
Will be filled by the K-Means batch job in a future iteration to power
opening-name fuzzy search in the React app.

---

## Stage 7 — Visualization

Four Grafana dashboards in [visualization/grafana/provisioning/dashboards/](visualization/grafana/provisioning/dashboards/).
Each reads from PostgreSQL via the provisioned `ChessDB` datasource.

| Dashboard | UID | What it shows |
|---|---|---|
| ELO Tracker | `elo-tracker` | Per-player ELO time series (Postgres `elo_history`) |
| Opening Trends | `opening-trends` | Stacked top-N openings per 5-min window (Postgres `opening_trends`) |
| Tournament Leaderboard | `tournament-leaderboard` | Top-20 standings + points distribution (Postgres `tournament_leaderboard`) |
| Batch Insights | `batch-insights` | Cluster pie + aggression histogram + per-player ALS recs (Postgres `opening_clusters`, `player_styles`, `als_vectors`) |
| Chess Pipeline Overview | `chess-overview` | Total games processed (originally Phase 0 placeholder, now points at `elo_history`) |

For our example game:

- **ELO Tracker** → pick `Ayman22` from the dropdown → time series shows 1364 at 23:00:02 with delta +11.
- **Opening Trends** → in the 23:00–23:05 window, `B10 Caro-Kann Defense: Two Knights Attack` appears with its share of games.
- **Tournament Leaderboard** → pick tournament `oSyPykCM` → table shows `Ayman22` and `daamien` with their running points.
- **Batch Insights** → cluster pie shows where B10 falls; pick `Ayman22` from the player dropdown → ALS recommendation table.

Default time range on the streaming dashboards: `2016-01-31T21:30Z – 2016-01-31T23:00Z`
(matches the PGN sample's actual played time).

---

## End-to-end timing for one game

For our example single game `OIR2O8JN`:

| Stage | Latency | What happens |
|---|---|---|
| 0 → 1 | <1 ms | parser yields the dict |
| 1 → 2 | ~1 ms | `to_event()` projection + Kafka send |
| 2 → 3 | <10 ms | Kafka acks (acks=1, in-cluster) |
| 3 → 4a (ELO tracker) | up to 10 s | next streaming trigger fires |
| 3 → 4b (opening trends) | up to 30 s | next streaming trigger + window flush |
| 3 → 4c (leaderboard) | up to 30 s | only if tournament game |
| 4 → 6 (storage) | <1 s per batch | foreachBatch sink writes |
| 6 → 7 (Grafana) | up to 10 s | dashboard refresh interval |

**Total worst case:** game lands in Kafka → visible on Opening Trends
dashboard ≈ **40 seconds**.

For batch ML jobs (5a-5c), the latency is "however long since the last
manual `spark-submit`". They reprocess the entire topic on each run.

---

## What's deferred (and why)

| Architecture box | Status | Reason |
|---|---|---|
| Stockfish enrichment producer | deferred | Out of scope. Standard rated PGN has no engine evals. Adding this means a Stockfish UCI container + a third producer. |
| Bot detection stream job | deferred | Depends on Stockfish (engine match rate). Reserved Cassandra `bot_scores` table exists. |
| Move NLP batch job | deferred | Standard rated dump has no commentary. Would ingest the Lichess annotated dataset separately. |
| `openings` Kafka feedback loop | deferred | The architecture's dashed feedback arrow: re-publish K-Means cluster IDs back into Kafka so streaming jobs can enrich live games. Trivial to add — one extra `df.write` in the K-Means job. |
| React + FastAPI front-end | deferred | Player-facing UI was not part of this iteration. Postgres + Elasticsearch reads are ready; just needs the API + UI. |
| Metabase analytical dashboards | deferred | Service is up at port 3003; same datasource as Grafana; just no dashboards built yet. |

---

## Reading guide

- [DEMO.md](DEMO.md) — runbook for live demos: what to click, what to say, recovery commands, anticipated Q&A.
- [README.md](README.md) — the original Phase 0 service map (still accurate for ports / URLs).
- [docker-compose.yml](docker-compose.yml) — every service definition.
- [storage/postgres/init.sql](storage/postgres/init.sql) — full Postgres schema.
- [storage/cassandra/init.cql](storage/cassandra/init.cql) — full Cassandra schema.
- The three streaming jobs ([stream_processing/](stream_processing/)) and three batch jobs ([batch_processing/](batch_processing/)) — each is one self-contained `.py` plus `submit.sh`. Read them in order: elo_tracker (simplest), opening_trends (windowing), leaderboard (stateful), then kmeans, player_styles, als.
