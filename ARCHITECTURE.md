# Architecture — Deep Dive

Complete reference for how the chess analytics pipeline is built. Read this
to understand the *why* behind every component. For runbook commands see
[DEMO.md](DEMO.md). For one game traced through every layer see
[PGN_TRACE.md](PGN_TRACE.md). For the presentation script see
[PRESENTATION.md](PRESENTATION.md).

---

## Table of contents

1. [The five-layer architecture](#1-the-five-layer-architecture)
2. [Layer 1 — Data sources](#2-layer-1--data-sources)
3. [Layer 2 — Ingestion](#3-layer-2--ingestion)
4. [Layer 3 — Message bus (Kafka)](#4-layer-3--message-bus-kafka)
5. [Layer 4 — Processing (Spark)](#5-layer-4--processing-spark)
6. [Layer 5 — Storage (polyglot)](#6-layer-5--storage-polyglot)
7. [Layer 6 — Visualization](#7-layer-6--visualization)
8. [Cross-cutting concerns](#8-cross-cutting-concerns)
9. [End-to-end data flows](#9-end-to-end-data-flows)
10. [Design decisions and trade-offs](#10-design-decisions-and-trade-offs)
11. [Failure modes and recovery](#11-failure-modes-and-recovery)
12. [Scalability analysis](#12-scalability-analysis)
13. [Deferred components](#13-deferred-components)

---

## 1. The five-layer architecture

The pipeline implements the canonical big-data architecture from the project
brief. Each layer has one responsibility and one well-defined contract with
the layer above and below it.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        LAYER 6 — VISUALIZATION                          │
│            Grafana (operator view)     Metabase (analyst view)          │
└─────────────────────────────────────────────────────────────────────────┘
                                  ↑
                                  │  reads
                                  │
┌─────────────────────────────────────────────────────────────────────────┐
│                          LAYER 5 — STORAGE                              │
│      Cassandra            PostgreSQL              Elasticsearch         │
│   (time-series)         (analytical cache)      (full-text search)      │
└─────────────────────────────────────────────────────────────────────────┘
                                  ↑
                                  │  writes (foreachBatch sinks)
                                  │
┌─────────────────────────────────────────────────────────────────────────┐
│                         LAYER 4 — PROCESSING                            │
│                                                                         │
│   Stream side (continuous)              Batch side (on demand)          │
│   ─────────────────────────              ────────────────────────        │
│   elo_tracker                            kmeans_openings                │
│   opening_trends                         player_styles                  │
│   tournament_leaderboard                 als_recommender                │
│   live_activity                                                         │
│   live_featured                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                  ↑
                                  │  subscribe (Spark Kafka connector)
                                  │
┌─────────────────────────────────────────────────────────────────────────┐
│                        LAYER 3 — MESSAGE BUS                            │
│                                                                         │
│      game-results  tourn-events  game-moves  player-events              │
│                                                                         │
│                       Apache Kafka 7.6.0                                │
└─────────────────────────────────────────────────────────────────────────┘
                                  ↑
                                  │  produce (kafka-python)
                                  │
┌─────────────────────────────────────────────────────────────────────────┐
│                          LAYER 2 — INGESTION                            │
│                                                                         │
│    Batch PGN producer                    Live stream producer           │
│    (Python + python-chess +              (Python + requests)            │
│     zstandard)                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                  ↑
                                  │  pulls
                                  │
┌─────────────────────────────────────────────────────────────────────────┐
│                        LAYER 1 — DATA SOURCES                           │
│                                                                         │
│   Lichess PGN dumps                   Lichess TV NDJSON API             │
│   database.lichess.org                lichess.org/api/tv/feed           │
│   ~1 GB compressed per month          per-game-per-move events          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key architectural properties:**

- **Loose coupling.** Every layer talks to the next via a stable contract
  (Kafka topic schemas, SQL/CQL schemas, REST APIs for Grafana/Metabase).
  Any layer can be swapped without touching the others.
- **Asymmetric persistence.** The same logical data lives in multiple stores
  tuned for their access patterns — see [Layer 5](#6-layer-5--storage-polyglot).
- **Dual ingestion.** Batch *and* stream sources flow through the same Kafka
  topics; downstream consumers don't know or care which producer wrote the
  event.
- **Single host, distributed pattern.** Everything runs on one machine via
  Docker Compose, but every component is the same one you'd deploy at scale.

---

## 2. Layer 1 — Data sources

We ingest two distinct data sources to cover both batch and stream patterns.

### 2.1 Lichess PGN dumps (batch source)

- **URL:** https://database.lichess.org/
- **Format:** Zstandard-compressed PGN (`.pgn.zst`)
- **Local sample:** [data/lichess_db_standard_rated_2016-02.pgn.zst](data/lichess_db_standard_rated_2016-02.pgn.zst)
- **Size:** ~867 MB compressed, ~3-5 GB uncompressed
- **Volume:** ~15-30 million games (Lichess publishes monthly)
- **Granularity:** one PGN block per game (headers + moves)

**Why this source:** Lichess publishes all rated games every month as a single
giant dump. It's the canonical historical-batch ingest scenario.

**What's in each game (PGN headers):**

```
[Event "Rated Blitz tournament https://lichess.org/tournament/oSyPykCM"]
[Site "https://lichess.org/OIR2O8JN"]
[Date "2016.01.31"]
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

1. e4 c6 2. Nf3 d6 3. Nc3 Nf6 4. d4 e5 ...
```

`%eval` (Stockfish per-move scores) and `%clk` (per-move clock times)
annotations may appear inline between moves but the **standard rated dump
does not contain `%eval`** — only `%clk`. This is why bot detection (which
needs engine match rates) is deferred: we'd need to ingest the separate
"evaluations" PGN flavor.

### 2.2 Lichess TV NDJSON API (live source)

- **URL:** https://lichess.org/api/tv/feed
- **Format:** NDJSON — one JSON object per line, infinite stream
- **Auth:** none (public endpoint)
- **Granularity:** one event per featured game switch, one event per move

**Why this source:** It's an actual real-time event stream — not a simulated
one. Lichess always has live games. The endpoint never returns; it stays
open and emits new events as they happen.

**Event types we observe:**

```json
{"t":"featured","d":{"id":"CPcqLAmN","orientation":"white",
  "players":[
    {"color":"white","user":{"name":"TonyGazzo","title":"GM"},"rating":3023,"seconds":60},
    {"color":"black","user":{"name":"AttackingBeast","title":"IM"},"rating":2910,"seconds":60}
  ],
  "fen":"...","wc":60,"bc":60}}

{"t":"fen","d":{"fen":"...","lm":"e2e4","wc":58,"bc":60}}
{"t":"fen","d":{"fen":"...","lm":"e7e5","wc":58,"bc":58}}
...
```

A `t=featured` event signals Lichess has switched to a new TV game. After
that, all `t=fen` events belong to that game until the next `featured` event
arrives. Our producer tracks the current `game_id` and tags every `fen`
event with it.

### 2.3 What's NOT a data source

- **No simulator.** We don't synthesize events. The pipeline ingests real
  data only.
- **No GraphQL / WebSocket APIs.** Lichess has them but they require auth.
- **No Lichess user profile API.** Would populate `players` table but
  requires per-player API calls — deferred.

---

## 3. Layer 2 — Ingestion

Two Python producers convert raw source data into Kafka messages.

### 3.1 Batch PGN producer

**Code:** [ingestion/batch_pgn_producer/](ingestion/batch_pgn_producer/)

```
main.py          ─── CLI entry point + Kafka send loop
pgn_parser.py    ─── PGN streaming parser (zstandard + python-chess)
requirements.txt
```

**Design:**

1. `parse_pgn(path)` is a generator that streams the .pgn.zst file. It uses
   `zstandard.ZstdDecompressor.stream_reader` so memory stays constant
   regardless of file size — never loads the full file.
2. For each game, `python-chess` parses headers and moves into a normalized
   Python dict (player names, ELOs, rating diffs, ECO, opening name, time
   control, tournament URL parsed from Event header).
3. `to_event()` projects the dict into the Kafka message schema. Skips games
   with missing ratings or timestamps.
4. `to_tourn_event()` projects to the tournament events schema if and only
   if the game had a tournament URL.
5. Both messages go through one `KafkaProducer` instance configured with
   `acks=1, linger_ms=20, batch_size=65536` — high-throughput, fire-and-forget.
6. A simple token-bucket rate limiter throttles to `--rate` messages/second
   so streaming jobs can keep up.

**CLI:**

```powershell
python -m ingestion.batch_pgn_producer.main `
  --pgn data/lichess_db_standard_rated_2016-02.pgn.zst `
  --bootstrap localhost:29092 `
  --topic game-results `
  --tourn-topic tourn-events `
  --rate 800 `
  --max-games 10000
```

**Kafka message schema (game-results topic):**

```json
{
  "game_id":       "OIR2O8JN",
  "played_at":     1454281202000,
  "white_player":  "Ayman22",
  "black_player":  "daamien",
  "white_elo":     1364,
  "black_elo":     1414,
  "white_diff":    11,
  "black_diff":    -11,
  "result":        "1-0",
  "eco":           "B10",
  "opening":       "Caro-Kann Defense: Two Knights Attack",
  "time_control":  300,
  "tournament_id": "oSyPykCM"
}
```

Message key: `game_id` (string, UTF-8). Same key always lands on the same
Kafka partition → idempotent replays, stable ordering for the same game.

### 3.2 Live stream producer

**Code:** [ingestion/stream_producer/main.py](ingestion/stream_producer/main.py)

**Design:**

1. Opens a `requests.get(LICHESS_TV_FEED, stream=True)` connection.
2. Iterates `resp.iter_lines()` — each line is one NDJSON event.
3. `featured_to_event()` builds a player-events message; `fen_to_event()`
   builds a game-moves message, tagged with the current `game_id` (tracked
   between events).
4. Both messages have `ingested_at = int(time.time() * 1000)` so downstream
   knows when the event arrived (vs when the game was originally played).
5. On any `requests.exceptions.RequestException`, exponential backoff up to
   60 s, then reconnect. `--max-reconnects -1` means unlimited.

**CLI:**

```powershell
python -m ingestion.stream_producer.main `
  --bootstrap localhost:29092 `
  --max-reconnects -1
```

**Kafka schemas:**

`player-events` (one per featured-game switch):
```json
{
  "event_type": "game_featured",
  "game_id":    "CPcqLAmN",
  "ingested_at": 1777500925711,
  "orientation": "white",
  "white": {"name":"TonyGazzo","title":"GM","rating":3023},
  "black": {"name":"AttackingBeast","title":"IM","rating":2910},
  "fen":  "rnbq...",
  "wc":   60, "bc": 60
}
```

`game-moves` (one per move played):
```json
{
  "event_type": "move",
  "game_id":    "CPcqLAmN",
  "ingested_at": 1777501109260,
  "fen":        "rnbqkbnr/pppppppp/8/8/8/5N2/PPPPPPPP/RNBQKB1R b KQkq - 1 1",
  "last_move":  "g1f3",
  "wc":         58, "bc": 60
}
```

Both keyed by `game_id` so all events for a TV game land on the same
partition.

### 3.3 Why two producers, not one

Different I/O patterns. The batch producer is *bursty* (reads as fast as
zstandard decodes), needs throttling, can run to completion. The stream
producer is *event-paced* (waits for the upstream API), runs forever,
needs reconnect logic. Forcing them into one process would either lose
throughput or complicate the code.

---

## 4. Layer 3 — Message bus (Kafka)

Kafka is the architectural backbone — every event in the pipeline passes
through it.

### 4.1 What Kafka actually does for us

Three distinct services:

1. **Producer-consumer decoupling.** Producers don't know how many consumers
   exist or where they are. Consumers don't know who produced. Adding a new
   consumer is zero-coordination work.

2. **Replay.** Kafka holds messages for the topic's retention period. Any
   new consumer can start from offset 0 and reprocess history. Our batch
   ML jobs exploit this — they re-read the entire `game-results` topic at
   submit time.

3. **Durability + ordering.** Within a partition, message order is preserved.
   Acks guarantee the message survived the broker's disk write. Survives
   consumer crashes.

### 4.2 Topic design

| Topic | Partitions | Key | Replication | Producer | Consumers |
|---|---|---|---|---|---|
| `game-results` | 3 | `game_id` | 1 | batch PGN | `elo_tracker`, `opening_trends`, 3 batch jobs |
| `tourn-events` | 3 | `tournament_id` | 1 | batch PGN (tournament games only) | `tournament_leaderboard` |
| `game-moves` | 6 | `game_id` | 1 | live stream | `live_activity` |
| `player-events` | 3 | `game_id` | 1 | live stream | `live_featured` |

**Partition count rationale:**

- 3 for medium-volume topics — enough parallelism for the streaming jobs
  without over-fragmenting state.
- 6 for `game-moves` because move events are ~30x higher volume than game
  events (one game produces 40+ move events).

**Key choice rationale:**

- All events for one logical entity (`game_id`, `tournament_id`) land on
  the same partition.
- That means stateful aggregations grouping by that key process events for
  one entity on one executor — no cross-partition state shuffling.
- For `tourn-events`, keying by `tournament_id` means all events for the
  same tournament land together — the leaderboard aggregation never needs
  to merge state across partitions.

**Replication factor = 1.** Single-node Kafka, no replicas. Production would
use 3 with `min.insync.replicas=2`. For this academic deployment,
the trade-off is acceptable — a broker crash loses unacknowledged messages
but topics are immediately recoverable.

### 4.3 Compose service

[docker-compose.yml](docker-compose.yml) defines:

```yaml
zookeeper:        # required by Kafka 7.x for cluster coordination
kafka:            # broker
kafka-ui:         # web UI at localhost:8080
```

Kafka exposes two listeners:

- **PLAINTEXT://kafka:9092** (in-cluster) — used by Spark and the UI.
- **PLAINTEXT_HOST://localhost:29092** (host-side) — used by the Python
  producers running on the host.

Two listeners are needed because container hostnames don't resolve outside
Docker. The producers run on the host (in the Python venv) for development
ergonomics; in production they'd be containerized and use the in-cluster
listener.

### 4.4 What's NOT in Kafka

- No schema registry. Message schemas are documented in code (the Spark
  `from_json` schemas) and the producer code. Production would use
  Confluent Schema Registry + Avro for type safety.
- No Kafka Connect. We don't sync Kafka to anything via Connect — Spark
  is the sole consumer.
- No transactions. Producers use `acks=1`, not exactly-once. Downstream
  upserts make this safe (see [§5.4](#54-exactly-once-vs-at-least-once)).

---

## 5. Layer 4 — Processing (Spark)

The processing layer has two halves: **streaming jobs** (continuous,
event-driven) and **batch jobs** (on-demand, snapshot the whole topic).

### 5.1 Spark cluster topology

```
┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐
│   spark-master      │         │   spark-worker-1    │         │   spark-worker-2    │
│   (1 container)     │ ─────── │   3 cores / 2 GB    │ ─────── │   3 cores / 2 GB    │
│   coordinates apps  │         │                     │         │                     │
└─────────────────────┘         └─────────────────────┘         └─────────────────────┘
```

- **Total cluster capacity:** 6 cores / 4 GB RAM
- **Spark version:** 3.5.6 on Bitnami image
- **Custom Dockerfile:** [spark/Dockerfile](spark/Dockerfile) — adds
  - `numpy` (required by MLlib)
  - `kafka-python`, `python-chess`, `cassandra-driver`, `psycopg2-binary`, `elasticsearch`, `faker`
  - JARs: `spark-sql-kafka-0-10`, `spark-cassandra-connector-assembly`,
    `postgresql JDBC`, `commons-pool2`, `spark-token-provider-kafka`
  - `/etc/passwd` entry for the `spark` user (works around a Hadoop JAAS
    NPE — see [§11.1](#111-jaas-getpwuid-null))

### 5.2 Streaming jobs (5)

All follow the same template:

```python
spark.readStream.format("kafka") \                  # subscribe
   .option("subscribe", TOPIC).load() \
   .select(from_json(value.cast("string"), SCHEMA).alias("e")) \
   .select("e.*") \
   .[transform, aggregate]() \                      # logic
   .writeStream.foreachBatch(write_batch) \         # sink
   .option("checkpointLocation", CHECKPOINT) \
   .trigger(processingTime="N seconds") \
   .start().awaitTermination()
```

| Job | File | Source | Pattern | Output mode | Watermark | Trigger | Sinks |
|---|---|---|---|---|---|---|---|
| `elo_tracker` | [stream_processing/elo_tracker/elo_tracker.py](stream_processing/elo_tracker/elo_tracker.py) | `game-results` | **stateless** explode | append | none | 10 s | Cassandra `chess.elo_history` + Postgres `elo_history` |
| `opening_trends` | [stream_processing/opening_trends/opening_trends.py](stream_processing/opening_trends/opening_trends.py) | `game-results` | **windowed** 5-min tumbling | update | 5 min | 30 s | Postgres `opening_trends` (psycopg2 upsert) |
| `tournament_leaderboard` | [stream_processing/tournament_leaderboard/leaderboard.py](stream_processing/tournament_leaderboard/leaderboard.py) | `tourn-events` | **stateful** `groupBy(tournament_id, player_id)` running sum | update | 1 hour | 30 s | Cassandra + Postgres (dual psycopg2 upsert) |
| `live_activity` | [stream_processing/live_activity/live_activity.py](stream_processing/live_activity/live_activity.py) | `game-moves` | **two queries** — 1-min throughput window + per-game running count | update | 2 min / 10 min | 30 s | Postgres `live_activity` + `game_lengths` |
| `live_featured` | [stream_processing/live_featured/live_featured.py](stream_processing/live_featured/live_featured.py) | `player-events` | **stateless** filter + project | append | none | 30 s | Postgres `featured_players` (upsert) |

**Detailed mechanics:**

#### 5.2.1 Stateless (`elo_tracker`, `live_featured`)

No `groupBy`, no aggregation. Each input event is transformed and written
out directly. No state store, no watermark.

- **`elo_tracker`:** Each `game-results` event becomes two output rows
  (one per player). This is an *explode*, not an aggregate. The job
  reads `white_diff` / `black_diff` directly from the message — that's
  why no state is needed. ELO delta was pre-computed by Lichess and
  embedded in the PGN.

- **`live_featured`:** Filters events where `event_type == 'game_featured'`
  and projects into the table schema. One Kafka message = one Postgres
  upsert.

#### 5.2.2 Windowed (`opening_trends`, half of `live_activity`)

Tumbling windows on event time. State is kept *per window* — when the
window closes (watermark passes), the state is evicted.

- `groupBy(window(event_time, "5 minutes"), eco).agg(count, ...)` 
- Watermark = 5 minutes after max event time seen
- `outputMode("update")` emits only windows whose counts changed in this
  micro-batch

#### 5.2.3 Stateful unbounded (`tournament_leaderboard`, half of `live_activity`)

`groupBy(key)` without a window. State is the entire `(key, accumulated)`
map, evicted only when the watermark indicates no more events for that
key will arrive.

- `groupBy(tournament_id, player_id).agg(sum, count, ...)`
- Watermark = 1 hour (tournaments are short-lived)
- For each player whose score changed, emit the new total → upsert

#### 5.2.4 Multi-query application (`live_activity`)

One Spark application runs two streaming queries against the same parsed
DataFrame. Both queries share the Kafka source (no duplicate consumption)
but maintain independent state and sinks.

```python
moves_df = readStream(game-moves).parse()

# Query A — 1-min tumbling throughput
moves_df.withWatermark(...)
        .groupBy(window(...))
        .agg(count, count_distinct)
        .writeStream.foreachBatch(write_activity).start()

# Query B — per-game running count
moves_df.withWatermark(...)
        .groupBy(game_id)
        .agg(count, min, max)
        .writeStream.foreachBatch(write_lengths).start()

spark.streams.awaitAnyTermination()
```

This is one of the more sophisticated Spark patterns in the project. Both
queries scale together (same executor) but their state machines are
independent.

### 5.3 Batch jobs (3)

Read Kafka in batch mode — i.e., `spark.read.format("kafka")` with
`endingOffsets=latest`. Effectively a snapshot of the topic at submit time.
Run via `spark-submit`, complete in seconds-to-minutes, exit.

| Job | File | Algorithm | Output |
|---|---|---|---|
| `kmeans_openings` | [batch_processing/kmeans_openings/kmeans_openings.py](batch_processing/kmeans_openings/kmeans_openings.py) | MLlib `KMeans(k=5)` on standardized per-ECO features | Postgres `opening_clusters` |
| `player_styles` | [batch_processing/player_styles/player_styles.py](batch_processing/player_styles/player_styles.py) | Aggregation per `player_id` with derived `aggression_index` | Postgres `player_styles` |
| `als_recommender` | [batch_processing/als_recommender/als_recommender.py](batch_processing/als_recommender/als_recommender.py) | MLlib `ALS(rank=20, implicitPrefs=True)` on `(player, ECO, plays)` matrix | Postgres `als_vectors` JSONB |

**Shared infrastructure:**

[batch_processing/common.py](batch_processing/common.py) provides:

- `build_spark(app_name)` — SparkSession builder
- `read_game_results(spark)` — batch Kafka read with the canonical schema
- `write_postgres(df, table, mode)` — JDBC writer

This eliminates duplication across the three batch jobs.

**Why MLlib over scikit-learn:**

- Native Spark integration — no DataFrame ↔ pandas conversion overhead
- Distributed by default — works across executors
- Same APIs whether running on 1 core or 1000

### 5.4 Exactly-once vs at-least-once

**Read path (Kafka → Spark):** exactly-once.
Spark's Kafka source uses idempotent offset commits in the checkpoint
directory. After a crash, the job resumes from the last committed offset
— no duplicates, no skips.

**Write path (Spark → Postgres / Cassandra):** at-least-once.
A `foreachBatch` write can succeed at the sink but fail before the offsets
commit. On retry, the same batch is re-written. This is safe because:

- Postgres sinks use `INSERT ... ON CONFLICT DO UPDATE` (idempotent upserts).
- Cassandra writes are naturally idempotent (PK upsert semantics).
- The append-only `elo_history` table uses a surrogate `BIGSERIAL` PK so
  duplicates are deduplicated at write time only by other means (we accept
  the minor duplication risk).

End-to-end this gives **effectively exactly-once** because the sinks are
idempotent — the same logical event lands in the same logical row regardless
of how many times Spark retries.

### 5.5 Resource isolation

5 streaming jobs share a 6-core / 4 GB cluster. Each is capped at:

```
--conf spark.cores.max=1
--conf spark.executor.cores=1
--conf spark.executor.memory=512m
```

Total streaming footprint: 5 cores / 2.5 GB → leaves 1 core / 1.5 GB
headroom for ad-hoc batch jobs.

Batch jobs use `--conf spark.cores.max=4` and run after pausing streaming
(documented in [DEMO.md §3](DEMO.md#step-7--run-a-batch-job-live-optional-2-min)).

---

## 6. Layer 5 — Storage (polyglot)

Three storage technologies, each chosen for a specific access pattern.

### 6.1 Apache Cassandra — time-series source of truth

**Image:** `cassandra:4.1`
**Schema:** [storage/cassandra/init.cql](storage/cassandra/init.cql)
**Keyspace:** `chess` (SimpleStrategy, RF=1)

**Tables:**

```cql
CREATE TABLE elo_history (
    player_id text,
    recorded_at timestamp,
    elo int,
    delta int,
    PRIMARY KEY (player_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC);

CREATE TABLE tournament_standings (
    tournament_id text,
    player_id text,
    points float,
    games_played int,
    wins int, draws int, losses int,
    last_updated timestamp,
    PRIMARY KEY (tournament_id, player_id)
);

CREATE TABLE game_moves (...);   -- schema in place, not currently populated
CREATE TABLE bot_scores (...);   -- schema in place, deferred job
```

**Design rationale:**

- **Partition key choice.** `elo_history` partitions by `player_id`.
  Every read for one player's history is one partition lookup — O(1)
  for partition discovery, sequential I/O within.
- **Clustering order DESC.** "Latest ELO for player X" is just
  `SELECT ... LIMIT 1` from the head of the clustered partition. No sort
  required.
- **No secondary indexes.** Cassandra penalizes secondary index queries
  heavily. Every access path is planned via PK.
- **Write-heavy by design.** Cassandra excels at append throughput.
  Compactions handle storage efficiency in the background.

**What goes here vs Postgres:** anything that's *primarily time-series*
or that we'd want to retain for years. Cassandra's storage model handles
billions of rows per node without read latency degradation.

### 6.2 PostgreSQL — analytical / presentation layer

**Image:** `postgres:16`
**Schema:** [storage/postgres/init.sql](storage/postgres/init.sql)

**Tables:**

| Table | Owner job | PK | Purpose |
|---|---|---|---|
| `elo_history` | `elo_tracker` (stream) | `BIGSERIAL id` | Per-player ELO event log (mirror of Cassandra) |
| `opening_trends` | `opening_trends` (stream) | `(window_start, eco)` | 5-min windowed game counts per opening |
| `tournament_leaderboard` | `tournament_leaderboard` (stream) | `(tournament_id, player_id)` | Live tournament standings |
| `opening_clusters` | `kmeans_openings` (batch) | `eco_code` | K-Means cluster assignments |
| `player_styles` | `player_styles` (batch) | `player_id` | Per-player metrics |
| `als_vectors` | `als_recommender` (batch) | `player_id` | ALS top-10 recommendations as JSONB |
| `live_activity` | `live_activity` (stream) | `window_start` | 1-min live throughput windows |
| `game_lengths` | `live_activity` (stream) | `game_id` | Per-game running move count |
| `featured_players` | `live_featured` (stream) | `game_id` | Snapshot of TV-featured players |
| `players` | (scaffold) | `player_id` | Reserved for future player-profile sync |
| `openings` | (scaffold) | `eco_code` | Reserved for opening win-rate aggregation |

**Why Postgres in addition to Cassandra:**

1. **Grafana support.** Grafana has a built-in Postgres datasource.
   Cassandra support exists only as an unsigned community plugin — extra
   ops friction. Dual-sink to Postgres eliminates that.

2. **JOINs.** Cassandra has no JOIN. Every analytical query that spans
   tables would need application-level join logic. Postgres handles it in
   one SQL statement.

3. **JSONB.** The ALS job stores top-10 recommendations as a JSON array.
   Postgres JSONB allows indexed queries like `factors->0->>'eco'` to
   extract the top recommendation in SQL.

4. **Window functions, aggregates, CTEs.** Used heavily in the Grafana
   panel SQL — Cassandra CQL doesn't support them.

5. **SQL familiarity.** Metabase and Grafana users write SQL. Translating
   to CQL would limit who could build dashboards.

**The `BIGSERIAL` workaround in `elo_history`:**

The original PK design was `(player_id, recorded_at)`. PGN timestamps are
per-second, and a player can finish two bullet games in the same UTC
second → JDBC INSERT rejected the duplicate. Switched to `BIGSERIAL id`
PK with `(player_id, recorded_at DESC)` as a secondary index. Cassandra
silently upserts so it's unaffected.

### 6.3 Elasticsearch — full-text search

**Image:** `elasticsearch:8.13.0`
**Mapping:** [storage/elasticsearch/mappings.json](storage/elasticsearch/mappings.json)
**Index:** `chess_openings` (text + keyword fields)

**Current state:** schema in place, **not currently populated**.

The intent is to populate from the K-Means batch job — emit each clustered
opening into Elasticsearch with the cluster label. Then a future React UI
could fuzzy-search opening names ("show me openings called 'Defense' in
the Drawish Positional cluster").

Kibana at http://localhost:5601 is connected for ad-hoc exploration.

### 6.4 Why polyglot, summarized

Different data has different access patterns:

| Access pattern | Best technology | Why |
|---|---|---|
| Append-only time-series, partition by entity | Cassandra | O(1) partition lookup, write-optimized |
| Analytical queries with JOINs and aggregations | Postgres | Mature SQL, indexes, JSONB |
| Free-text search across long fields | Elasticsearch | Inverted index, tokenization, ranking |

Forcing all three patterns into one store would compromise at least one.
The cost is operational complexity (three systems to monitor) — acceptable
because each plays to its strengths.

---

## 7. Layer 6 — Visualization

Two tools, deliberately split by use case.

### 7.1 Grafana — operator view

**Image:** `grafana/grafana:10.4.0`
**Port:** 3000
**Auth:** `admin` / `admin123` (from `.env`)
**Datasource:** Postgres, provisioned from
[visualization/grafana/provisioning/datasources/postgres.yaml](visualization/grafana/provisioning/datasources/postgres.yaml)

**Dashboards** (provisioned via YAML on container start):

| Dashboard | UID | Reads | What it shows |
|---|---|---|---|
| Chess Pipeline Overview | `chess-overview` | `elo_history` | Total games processed (stat) |
| ELO Tracker | `elo-tracker` | `elo_history` | Per-player ELO time series + stats |
| Opening Trends | `opening-trends` | `opening_trends` | Stacked top-N openings per 5-min window |
| Tournament Leaderboard | `tournament-leaderboard` | `tournament_leaderboard` | Top-20 standings + points histogram |
| Batch Insights | `batch-insights` | `opening_clusters`, `player_styles`, `als_vectors` | Cluster pie, aggression histogram, ALS recommendations |

**Default time range:** `2016-01-31T21:30Z – 2016-01-31T23:00Z` on the
streaming dashboards (matches the PGN sample's played-time).

**Why Grafana:**

- Time-series rendering is best-in-class
- Dashboards-as-code (provisioning JSON files)
- Templating variables (e.g. `$player` dropdown re-runs queries)
- Operator-friendly auto-refresh (10s default)

### 7.2 Metabase — analyst view

**Image:** `metabase/metabase:v0.49.0`
**Port:** 3003
**Auth:** `admin@chess.local` / `Chess123!` (configured by [metabase/setup.sh](metabase/setup.sh))
**Metadata store:** separate Postgres container `metabase-db` (don't confuse
with the main `postgres` container that holds the analytical data)
**Datasource:** Same `chessdb` Postgres as Grafana

**Setup automation:**

[metabase/setup.sh](metabase/setup.sh) is idempotent:

1. Hits `GET /api/session/properties` — checks `has-user-setup`.
2. If no admin yet: POST `/api/setup` with credentials from `.env` + the
   Postgres connection details. Creates admin and registers ChessDB
   datasource in one call.
3. If admin exists: log in with `.env` creds, check whether ChessDB is
   listed. If yes, exit; if no, POST `/api/database` to add it.

**Dashboards** (built by the user post-setup, SQL provided in
[DEMO.md §5b](DEMO.md#step-5b--switch-to-metabase-for-live--cross-table-views-3-5-min)):

- **Live Stream**: moves/min, active games, featured-players table,
  rating histogram, game-length histogram.
- **Cross-table analytics**: top aggressive players joined with their ALS
  recommendation and cluster label; games-per-cluster joining `opening_trends`
  with `opening_clusters`.

**Why Metabase:**

- SQL editor is more capable than Grafana's (visible execution plans,
  better autocomplete)
- Native JOIN ergonomics in the question builder
- "Browse data" mode lets analysts explore tables without writing SQL
- Better suited for ad-hoc questions vs always-on dashboards

### 7.3 Why both

| | Grafana | Metabase |
|---|---|---|
| Time-series rendering | ★★★★ | ★★ |
| SQL editor | ★★ | ★★★★ |
| JOIN ergonomics | ★ | ★★★★ |
| Auto-refresh / alerting | ★★★★ | ★★ |
| Ad-hoc exploration | ★ | ★★★★ |

Picking one would compromise. Picking both — at the cost of one extra
container — gives both communities what they want.

### 7.4 Kibana

**Image:** `kibana:8.13.0`
**Port:** 5601

Connected to Elasticsearch but currently empty. Useful for verifying ES
index mappings during development.

---

## 8. Cross-cutting concerns

### 8.1 Networking

Single Docker network `chess-net` (custom bridge). All inter-container
communication uses service hostnames (e.g. `kafka:9092`, `postgres:5432`).
Two external listener exceptions:

- Kafka exposes `localhost:29092` for host-side Python producers
- Postgres exposes `localhost:5434` for host-side `psql` / debugging

Service-to-service communication never leaves the bridge network.

### 8.2 Configuration management

[.env](.env) holds:

- Image versions (pinned, no `latest`)
- Service ports (so they're tunable without editing the compose file)
- Credentials (POSTGRES_USER/PASSWORD, GRAFANA admin, METABASE admin)
- JVM flags (Elasticsearch heap)
- Kafka broker config

Compose interpolation uses `${VAR}` references. No secrets in compose file
itself.

### 8.3 Resource budget

| Service | CPU | RAM |
|---|---|---|
| zookeeper | 0.1 | 256 MB |
| kafka | 1 | 1 GB |
| spark-master | 0.5 | 1 GB |
| spark-worker-1 | 3 | 2 GB |
| spark-worker-2 | 3 | 2 GB |
| cassandra | 1 | 1 GB |
| postgres | 0.5 | 512 MB |
| metabase-db | 0.5 | 256 MB |
| elasticsearch | 1 | 512 MB heap |
| grafana | 0.2 | 256 MB |
| metabase | 0.5 | 512 MB |
| kibana | 0.5 | 512 MB |
| kafka-ui | 0.2 | 256 MB |

Total: roughly **12 cores / 10 GB** for comfortable operation, but actually
runs on a 4-core / 8 GB laptop because most services idle.

### 8.4 Observability

- **Spark UI** (http://localhost:8090) — running applications, executor
  status, query progress.
- **Kafka UI** (http://localhost:8080) — topic browser, consumer group lag,
  message inspection.
- **Grafana** for the data flowing through the pipeline.
- **Container logs** via `docker logs <service>` for individual service
  troubleshooting.

No Prometheus / Alertmanager in this iteration — would be the production
addition.

### 8.5 Security

This is an academic deployment. **Not production-grade:**

- Kafka has no authentication or TLS
- Postgres uses a hardcoded password from `.env`
- Cassandra has no authentication
- Elasticsearch security explicitly disabled (`xpack.security.enabled: false`)
- Grafana / Metabase use admin/password with no MFA

All services are bound to localhost or the internal Docker network — no
external exposure. Production would add: mTLS on Kafka, IAM-style roles
on every datastore, OAuth on Grafana/Metabase, Vault for secrets.

---

## 9. End-to-end data flows

Three worked examples of how a piece of data moves through the system.

### 9.1 A historical PGN game becoming a Grafana ELO line

```
PGN file (zstandard)
   │  parse_pgn() in pgn_parser.py
   ▼
{game_id, white_player=Ayman22, white_elo=1364, white_diff=+11, ...}
   │  to_event() in batch_pgn_producer/main.py
   ▼
KafkaProducer.send("game-results", key=game_id, value=JSON)
   │  (in-network)
   ▼
Kafka topic "game-results", partition by hash(game_id)
   │  Spark Structured Streaming, subscribe("game-results")
   ▼
elo_tracker.py: from_json + explode into 2 player rows
   │  foreachBatch(write_batch)
   ▼
   ├─→ Cassandra chess.elo_history (INSERT)
   └─→ Postgres elo_history (JDBC INSERT, BIGSERIAL id)
   │
   │  Grafana polls Postgres every 10s
   ▼
ELO Tracker dashboard renders a new data point
```

End-to-end latency: ~10-15 seconds from PGN read to Grafana panel update,
dominated by the Spark trigger interval.

### 9.2 A live Lichess TV move becoming a Metabase row

```
HTTP NDJSON stream from lichess.org/api/tv/feed
   │  requests.iter_lines() in stream_producer/main.py
   ▼
JSON line: {"t":"fen","d":{"fen":"...","lm":"e2e4","wc":...}}
   │  fen_to_event() with ingested_at = now() ms
   ▼
KafkaProducer.send("game-moves", key=current_game_id, value=JSON)
   │
   ▼
Kafka topic "game-moves"
   │  Spark Structured Streaming, subscribe("game-moves")
   ▼
live_activity.py: parse with watermark, two streaming queries
   ├─→ Query A: 1-min tumbling window → live_activity(window_start, moves_count, active_games)
   └─→ Query B: groupBy(game_id) → game_lengths(game_id, move_count, first_seen, last_seen)
   │  foreachBatch with psycopg2 ON CONFLICT upsert
   ▼
Postgres live_activity / game_lengths
   │  Metabase queries on demand
   ▼
Live Stream dashboard renders next refresh
```

Latency from move played to Metabase question result: ~30-60 seconds.

### 9.3 The batch ALS job producing recommendations

```
Kafka "game-results" topic (entire history)
   │  spark.read.format("kafka") with endingOffsets=latest
   ▼
DataFrame: all game-results events as JSON
   │  from_json + select
   ▼
build_interactions(): (player_id, eco) → plays
   │  filter active players (≥5 games) and common ECOs (≥5 games)
   ▼
StringIndexer for player_id and eco → integer IDs
   │
   ▼
MLlib ALS(rank=20, implicit, maxIter=10).fit()
   │
   ▼
model.recommendForAllUsers(10) → top-10 recs per player_idx
   │  JSON-serialize the (eco, score) list per player
   ▼
Postgres als_vectors_stage (JDBC INSERT as TEXT)
   │  psycopg2: INSERT INTO als_vectors SELECT factors::jsonb FROM als_vectors_stage
   ▼
Postgres als_vectors(player_id, factors JSONB, updated_at)
   │  Grafana "Batch Insights" dashboard queries it
   ▼
Recommendations panel shows per-player top-10 ECOs
```

Total batch runtime: ~45 seconds on our 14k-interaction dataset.

---

## 10. Design decisions and trade-offs

The key decisions, the alternatives we considered, and why we chose what
we did.

### 10.1 Kafka vs alternatives

**Considered:** Kafka, RabbitMQ, Redis Streams, plain HTTP / gRPC.

**Chose Kafka because:**
- Append-only log model fits chess events (sequential, replayable)
- Native Spark connector with checkpoint integration
- Replay support — batch jobs reprocess history on demand
- Industry standard for the use case

**Trade-offs:**
- ZooKeeper dependency (Kafka 7.x; newer KRaft-only deployments skip it)
- Heavier than RabbitMQ for very simple cases
- More features than we use (transactions, schema registry)

### 10.2 Spark Structured Streaming vs alternatives

**Considered:** Spark Structured Streaming, Spark DStreams (legacy),
Flink, Kafka Streams.

**Chose Spark Structured Streaming because:**
- Unified API for stream and batch — same code reads Kafka as a stream
  *or* a batch
- MLlib for batch ML jobs uses the same SparkSession
- DataFrames API is far more ergonomic than DStreams (RDDs)
- Course familiarity

**Trade-offs:**
- Higher minimum latency than Flink (Spark micro-batches; Flink is
  per-event)
- Heavier resource footprint than Kafka Streams (which runs in the
  application JVM)

### 10.3 Cassandra + Postgres vs single store

**Considered:** Cassandra only, Postgres only, Cassandra + Postgres,
ScyllaDB, CockroachDB.

**Chose Cassandra + Postgres because:**
- Cassandra is the natural choice for time-series (write-optimized,
  partitioned)
- Postgres is the natural choice for analytical reads (SQL, JSONB, JOINs)
- Grafana support is much better with Postgres
- Doubling storage cost is acceptable for the access-pattern flexibility

**Trade-offs:**
- Data duplication (logically same data in two stores)
- Schema drift risk (kept in sync only by the streaming jobs writing
  consistently)
- More moving parts to monitor

### 10.4 Grafana + Metabase vs one tool

**Considered:** Grafana only, Metabase only, Superset, custom React UI.

**Chose Grafana + Metabase because:**
- Each has clear strengths the other lacks
- Both already in the compose stack (Metabase came as a Phase 0 service)
- Building a React UI is presentation polish, not architecture

### 10.5 Manual `spark-submit` vs Airflow

**Considered:** Manual scripts, Airflow, cron in container.

**Chose manual `spark-submit` because:**
- Academic scope — batch jobs run on demand, not on a cycle
- Airflow adds two services (webserver + scheduler) for one DAG
- Cron in container is fragile (no UI, no retries, no observability)

**Future:** Airflow is the right answer at production scale.

### 10.6 PGN replay vs simulating live data

**Considered:** Replay historical PGN, simulate live data, fetch real
Lichess API only.

**Chose all three:**
- PGN replay covers the batch path
- Real Lichess TV API covers the live path
- They write to different Kafka topics — no conflict

**Subtle decision:** the live producer does NOT feed `game-results`. Its
events go to `game-moves` and `player-events`. This means ELO tracker
never sees live data — a deliberate architectural simplification we
documented honestly.

### 10.7 K-Means K=5

**Considered:** k=3, 5, 7, 10, dynamically chosen via silhouette score.

**Chose k=5 because:**
- 5 produced clean, human-readable cluster separations on inspection
- Maps onto recognizable chess opening categories (sharp / drawish / etc.)
- Academic scope — silhouette analysis was out of budget

**Honest weakness:** the labels are post-hoc and subjective. Production
would validate with chess-domain expert review.

### 10.8 ALS rank=20 with implicit feedback

**Considered:** rank ∈ {10, 20, 50, 100}, explicit vs implicit feedback.

**Chose rank=20 with implicit because:**
- Implicit is correct for the data — play counts are confidence signals,
  not ratings
- rank=20 is the MLlib default and well-tuned for moderate dataset sizes
- Higher rank overfits on our 14k-interaction dataset
- Lower rank loses signal

---

## 11. Failure modes and recovery

How each layer fails and how we handle it.

### 11.1 JAAS `getpwuid` null

**Layer:** Spark
**Symptom:** Streaming job crashes immediately with
`KerberosAuthException: failure to login: NullPointerException: invalid null input: name`
**Root cause:** Bitnami Spark image runs as UID 1001 with no `/etc/passwd`
entry. Hadoop's JAAS `UnixLoginModule` calls `getpwuid(1001)` and gets
null.
**Fix:** Append `spark:x:1001:0:Spark user:/tmp:/bin/bash` to `/etc/passwd`
in [spark/Dockerfile](spark/Dockerfile).

### 11.2 Spark `spark.jars.ivy` is `?/.ivy2`

**Layer:** Spark
**Symptom:** `IllegalArgumentException: basedir must be absolute: ?/.ivy2/local`
on every spark-submit.
**Root cause:** Same UID 1001 has no home directory; `~` expands to `?`.
**Fix:** Every submit script passes `--conf spark.jars.ivy=/tmp/.ivy2`.

### 11.3 Per-second PK collision in Postgres `elo_history`

**Layer:** Storage
**Symptom:** JDBC INSERT fails with
`duplicate key value violates unique constraint "elo_history_pkey"`.
**Root cause:** PGN timestamps are per-second; same player finishes two
bullet games in the same UTC second.
**Fix:** Switched PK to surrogate `BIGSERIAL id`; original `(player_id,
recorded_at)` is now a non-unique index.

### 11.4 Spark JDBC doesn't support JSONB

**Layer:** Storage (ALS sink)
**Symptom:** ALS factors written as TEXT, not queryable with `factors->`
operator.
**Root cause:** Spark's JDBC writer maps to standard SQL types only.
**Fix:** ALS job writes to a staging table, then a follow-up psycopg2 call
runs `INSERT INTO als_vectors SELECT factors::jsonb ...`.

### 11.5 Resource starvation with 5 streams

**Layer:** Spark cluster
**Symptom:** A submitted job stays in `WAITING` state with "Initial job
has not accepted any resources".
**Root cause:** Each Spark app defaults to `executor.memory=1g`. With 5
streams × 1 GB on 4 GB total worker memory, the 5th couldn't fit.
**Fix:** Every stream submit script sets `--conf spark.executor.memory=512m`.
5 × 512 MB = 2.5 GB on 4 GB cluster, comfortable headroom.

### 11.6 Live producer disconnects

**Layer:** Ingestion
**Symptom:** TCP connection to Lichess TV drops after some time.
**Root cause:** Lichess servers close idle connections, network blips, etc.
**Fix:** [ingestion/stream_producer/main.py](ingestion/stream_producer/main.py)
has built-in exponential backoff up to 60s, retries indefinitely with
`--max-reconnects -1`.

### 11.7 Streaming checkpoint lost on container restart

**Layer:** Spark
**Symptom:** After a Spark container restart, the streaming jobs resume
from `latest` Kafka offsets — meaning they miss whatever arrived during
the downtime.
**Root cause:** Checkpoints stored at `/tmp/checkpoints/` which is volatile
in the container.
**Status:** Known limitation. Fix: mount a Docker volume on `/tmp/checkpoints`
in [docker-compose.yml](docker-compose.yml). One-line change. Not done
because the demo data is reproducible.

---

## 12. Scalability analysis

How the architecture would behave under increasing load.

### 12.1 Ingestion scaling

- **Batch producer:** I/O bound on zstandard decode. Linear with file size.
  Multiple producers can read different monthly dumps in parallel,
  partitioned across Kafka topics.
- **Live producer:** Bound by Lichess API throughput. Lichess publishes
  ~1 TV game per minute on average. Adding more API endpoints
  (`/api/stream/games-by-users`) scales horizontally.

### 12.2 Kafka scaling

- Single broker handles ~10k msg/sec easily. Our pipeline produces ~1k/sec
  throttled.
- Adding brokers: increase replication factor, redistribute partitions.
- Adding partitions: increase per-topic parallelism. Requires careful key
  selection to avoid hot partitions.
- **Real bottleneck at scale:** disk I/O. Kafka is fundamentally a
  sequential-write append log.

### 12.3 Spark scaling

- Streaming jobs are partition-parallel — each Kafka partition gets one
  Spark task. 3-partition topic → 3 parallel tasks → 3 cores fully
  utilized.
- Adding more workers: more cores, more parallel tasks. Spark
  auto-distributes.
- State store: keyed by partition. Adding partitions splits state
  proportionally.
- **Real bottleneck at scale:** state store memory. Stateful aggregations
  hold per-key state in memory; cardinality is the limit.

### 12.4 Storage scaling

- **Cassandra:** linear scale by adding nodes. Partitions distribute
  across nodes via consistent hashing.
- **Postgres:** vertical scaling first (bigger machine), then read
  replicas, then partitioning. JOINs and JSONB make horizontal
  partitioning harder than Cassandra.
- **Elasticsearch:** linear scale by adding nodes. Shards distribute.

### 12.5 The real bottleneck

For our specific dataset (10k messages/sec at full throttle, ~30k games
in topic, sub-second batch latency), **none of the layers are bottlenecked**.
The single-laptop Docker Compose stack handles this load with room to
spare.

The first bottleneck under load would be **Postgres write throughput** as
all streaming jobs write to it. The fix is sharding by table — move time-
series writes off Postgres entirely (rely on Cassandra) and use Postgres
only for the smaller analytical tables.

---

## 13. Deferred components

What's explicitly out of scope, why, and what it would take.

### 13.1 Stockfish enrichment + bot detection

- **What:** A Stockfish UCI container, an enrichment producer that
  computes engine match rate per move, and a `bot_detection` streaming
  job consuming `game-moves` + Stockfish output.
- **Why deferred:** Stockfish containerization is non-trivial, computing
  engine evals at scale needs careful caching, and the standard rated PGN
  has no embedded evals.
- **Effort estimate:** 2-3 days. Reserved table `bot_scores` already in
  the Cassandra schema.

### 13.2 Move NLP

- **What:** Spark NLP processing game commentary text.
- **Why deferred:** The standard rated PGN dump has no commentary fields.
  Would need to ingest the Lichess annotated dataset (separate Kaggle
  source) as a third producer.
- **Effort estimate:** 1-2 days for ingest + tokenization, plus model
  training time.

### 13.3 React + FastAPI opening explorer

- **What:** Player-facing UI that reads ALS recs from Postgres and fuzzy-
  searches openings from Elasticsearch.
- **Why deferred:** Metabase gives analytical reach for free; building a
  custom UI is presentation polish.
- **Effort estimate:** 3-5 days for a minimal React app, more for
  production quality.

### 13.4 K-Means feedback loop into `openings` Kafka topic

- **What:** Re-publish K-Means cluster IDs to a new `openings` Kafka topic
  so streaming jobs can enrich live games with cluster labels in real
  time. This is the dashed feedback arrow in the original architecture
  diagram.
- **Why deferred:** One extra `df.write` in the K-Means job, but requires
  the streaming jobs to consume from a second topic. Architecturally
  significant but small in code volume.
- **Effort estimate:** ~2 hours.

### 13.5 Airflow / orchestration

- **What:** Replace manual `spark-submit` with Airflow DAGs (one per batch
  job), scheduled daily.
- **Why deferred:** Manual scripts are sufficient for academic scope.
- **Effort estimate:** 1 day to scaffold Airflow + write three DAGs.

### 13.6 Streaming checkpoint persistence

- **What:** Docker volume on `/tmp/checkpoints` so streaming jobs survive
  container restarts without losing offsets.
- **Why deferred:** Demo data is reproducible.
- **Effort estimate:** 1 line of `docker-compose.yml`, 5 minutes.

### 13.7 Schema registry

- **What:** Confluent Schema Registry + Avro for typed Kafka messages.
- **Why deferred:** Academic scope. JSON with documented schemas in code
  is sufficient.
- **Effort estimate:** 1 day to migrate all producers and consumers.

### 13.8 Authentication / security

- **What:** mTLS on Kafka, RBAC on every datastore, OAuth on Grafana /
  Metabase, secrets in Vault.
- **Why deferred:** Academic deployment, single host, no external exposure.
- **Effort estimate:** 1-2 weeks for a complete security pass.

---

## Where each piece lives — file map

```
big-data-project-lichess/
├── docker-compose.yml                   ← 14 services, chess-net, healthchecks
├── .env                                 ← all ports, versions, credentials
├── data/                                ← Lichess PGN sample
│
├── spark/Dockerfile                     ← bitnami spark + numpy + JARs + /etc/passwd fix
│
├── ingestion/
│   ├── batch_pgn_producer/              ← reads PGN → game-results + tourn-events
│   │   ├── main.py
│   │   ├── pgn_parser.py
│   │   └── requirements.txt
│   └── stream_producer/                 ← reads Lichess TV → player-events + game-moves
│       └── main.py
│
├── stream_processing/                    ← 5 streaming jobs, each with submit.sh
│   ├── elo_tracker/
│   ├── opening_trends/
│   ├── tournament_leaderboard/
│   ├── live_activity/
│   └── live_featured/
│
├── batch_processing/                     ← 3 batch jobs, each with submit.sh
│   ├── common.py                         ← shared SparkSession + Kafka reader
│   ├── kmeans_openings/
│   ├── player_styles/
│   └── als_recommender/
│
├── storage/
│   ├── cassandra/init.cql                ← 4 tables: game_moves, elo_history, bot_scores, tournament_standings
│   ├── postgres/init.sql                 ← 11 tables, see Layer 5 §6.2
│   └── elasticsearch/mappings.json
│
├── visualization/
│   └── grafana/provisioning/
│       ├── datasources/postgres.yaml
│       └── dashboards/                   ← 5 JSON dashboards
│
├── metabase/setup.sh                     ← idempotent admin user + datasource setup
│
└── (docs)
    ├── DEMO.md                           ← runbook for the live demo
    ├── PGN_TRACE.md                      ← one game traced through every layer
    ├── PRESENTATION.md                   ← talk script with Q&A
    └── ARCHITECTURE.md                   ← this document
```
