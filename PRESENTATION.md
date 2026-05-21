# Chess Pipeline — Presentation Script

**For tomorrow's defense.** Read once before walking in. The "Talk track" lines
are roughly what to say; the **Q&A** blocks anticipate what the teacher might
ask after each part.

Total presentation time target: **15–20 minutes** + 5–10 minutes of Q&A.

---

## Opening (1 min)

> "Lichess is the world's second-largest chess server. Every month they publish
> a PGN dump of all rated games — about 100 million games, around 1 GB
> compressed per month. They also expose a public NDJSON API streaming live
> games in real time. The challenge is: how do we ingest both sources, process
> them with batch *and* stream patterns, store the results in databases tuned
> to different access patterns, and visualize for both operators and analysts —
> end-to-end, on a single laptop? That's what we built."

> "Our architecture has five layers — data sources, ingestion, message bus,
> processing, storage, and visualization — and it runs entirely on
> Docker Compose: 14 services, no cloud."

**If asked at the start "Why chess?":**
> Two reasons. (1) The data is genuinely event-driven — every move is a
> timestamped event with rich structure. (2) Lichess offers both batch and
> stream sources natively, so we get both ingestion patterns for free without
> simulating either.

---

## Part 1 — Architecture overview (1 min)

> "These are the five layers from the architecture diagram. Each box maps to
> one or more concrete services."

```
data sources    →   Lichess PGN dump  +  Lichess TV live NDJSON API
ingestion       →   2 Python producers (batch PGN, live stream)
message bus     →   Apache Kafka (5 topics)
processing      →   5 Spark Structured Streaming jobs  +  3 Spark MLlib batch jobs
storage         →   Cassandra (time-series)  +  PostgreSQL (relational/analytics)  +  Elasticsearch (search)
visualization   →   Grafana (operator view)  +  Metabase (analyst view)
```

> "Everything runs in Docker on a custom bridge network called `chess-net`.
> One `docker compose up -d` brings all 14 services online with healthchecks."

### Q&A

**Q: Why five layers and not four?**
> A clean separation of concerns. Ingestion knows how to read the source;
> Kafka decouples producers from consumers; processing is stateless if we want
> to scale horizontally; storage is polyglot because different data has
> different access patterns; visualization is a thin read layer. Each layer
> can be swapped independently — for example, replacing Kafka with Kinesis
> wouldn't touch any other layer.

**Q: How would this scale?**
> Three scale axes. (1) Kafka partitions: increase from 3 → 30 and add Spark
> executor cores. (2) Spark workers: add containers. (3) Storage: Cassandra
> shards by partition key natively, Postgres needs read replicas + connection
> pooling. The streaming jobs are already partitioned by `game_id` /
> `tournament_id` / `player_id` so they scale horizontally.

---

## Part 2 — Data sources (1 min)

> "We have two distinct data sources, batch and stream, mapping to the two
> data-engineering patterns the course covers."
>
> "Batch source: `data/lichess_db_standard_rated_2016-02.pgn.zst` — the
> February 2016 standard rated games dump from `database.lichess.org`. 867 MB
> compressed, expands to roughly 3-5 GB and contains tens of millions of
> games. Each game is a PGN block with headers (Event, Site, White, Black,
> Result, ECO, etc.) and a move list."
>
> "Stream source: `https://lichess.org/api/tv/feed`. A public NDJSON endpoint
> that emits an event whenever Lichess TV switches games (a `featured` event),
> and one event per move played (`fen` events). No authentication required."

### Q&A

**Q: Why this specific PGN month?**
> February 2016 was the sample we had locally. The pipeline isn't time-locked
> — point the producer at any monthly dump and it works the same.

**Q: Why not the current month?**
> No technical reason. We picked a smaller historical month so the demo
> doesn't pause on a multi-gigabyte download.

**Q: What's in a PGN file exactly?**
> Headers in `[Tag "Value"]` form, followed by the moves in algebraic
> notation. Headers include the players, both ELOs and rating-diffs, ECO
> opening code, time control, termination reason, and timestamps in UTC. Some
> Lichess dumps include `%eval` and `%clk` annotations between moves — Stockfish
> evaluation and remaining clock time. The standard rated dump doesn't have
> evals; the parser handles both formats.

---

## Part 3 — Ingestion (2 min)

> "We have two Python producers, one per source. Both write to Kafka."
>
> "Producer 1 — `ingestion/batch_pgn_producer/`. It streams the
> Zstandard-compressed PGN via the `zstandard` library, parses each game with
> `python-chess`, and emits one JSON message per game to the `game-results`
> Kafka topic. If the game is a tournament game (the `Event` header contains
> a Lichess tournament URL), it also emits a second message to `tourn-events`.
> Rate-limited via a token bucket so downstream streams have time to process."
>
> "Producer 2 — `ingestion/stream_producer/`. It holds an HTTP connection to
> the Lichess TV NDJSON feed. Each `t=featured` event becomes a message on
> `player-events`; each `t=fen` move event becomes a message on `game-moves`.
> Auto-reconnects with exponential backoff if Lichess drops the stream."

### Q&A

**Q: Why Python for the producers?**
> Lichess publishes a mature Python chess library (`python-chess`) that
> handles PGN parsing, including edge cases like the `%eval` and `%clk`
> annotations. Writing the parser from scratch would have been 2-3 days of
> work for no benefit.

**Q: What happens if Kafka is down when the producer runs?**
> The `kafka-python` client retries with exponential backoff. With `acks=1`
> we lose messages only if the leader broker crashes before replication —
> acceptable for an academic demo. Production would use `acks=all` and a
> replication factor > 1.

**Q: How fast is the producer?**
> About 800–1000 messages per second on a laptop, intentionally throttled.
> The bottleneck is the rate limiter, not Kafka. Without throttling, easily
> 5000+ messages/second.

**Q: Why throttle?**
> So the streaming jobs can keep up during the demo. With 5000 msg/s the
> jobs would lag and you'd see Grafana panels delay. Throttled, the lag is
> consistently under a second.

---

## Part 4 — Kafka (2 min)

> "Kafka is the architectural backbone — not just a queue. It does three
> things for us. First, it decouples producers from consumers. Our batch
> producer and stream producer don't know or care that five different Spark
> jobs are reading their output. Second, it provides replay: any consumer can
> start from offset zero and reprocess everything. Third, it provides
> durability — events survive consumer crashes."
>
> "Five topics, with explicit partitioning:"

| Topic | Partitions | Producer | Consumers |
|---|---|---|---|
| `game-results` | 3 | batch PGN | `elo_tracker`, `opening_trends`, all 3 batch jobs |
| `tourn-events` | 3 | batch PGN (when tournament) | `tournament_leaderboard` |
| `game-moves` | 6 | live stream | `live_activity` |
| `player-events` | 3 | live stream | `live_featured` |

> "Keys are `game_id`, `tournament_id`, or `game_id` again — this keeps all
> events for the same logical entity on the same partition, so stateful
> aggregations don't cross-shard."

### Q&A

**Q: Why Kafka and not RabbitMQ or Redis Streams?**
> Kafka's append-only log model fits this domain perfectly: chess events are
> sequential and we want full replay. RabbitMQ is queue-oriented — once a
> message is consumed, it's gone. Redis Streams works but has weaker
> durability guarantees and no native Spark connector.

**Q: Why three partitions on `game-results` and six on `game-moves`?**
> `game-moves` has higher event volume (one per move vs one per game) so more
> partitions give more parallelism. We sized partitions to roughly match
> expected throughput per partition.

**Q: What's a consumer offset?**
> Each Spark streaming job tracks its position in each topic-partition pair.
> Stored in the job's checkpoint directory. When a job restarts, it resumes
> from the last committed offset — no duplicates, no skips.

**Q: How does Kafka handle backpressure?**
> Producers throttle naturally because Kafka returns slower acks under load.
> Consumers control their own pace by polling — Kafka doesn't push. Spark's
> `maxOffsetsPerTrigger` parameter caps per-batch consumption if needed.

---

## Part 5 — Stream processing (3 min)

> "Five Spark Structured Streaming jobs run continuously on the cluster. All
> follow the same pattern: read from Kafka, parse JSON with a schema,
> aggregate or project, write to Postgres and/or Cassandra via `foreachBatch`.
> They differ in what *kind* of aggregation they do."

| Job | Pattern | Watermark | Sink |
|---|---|---|---|
| `elo_tracker` | **stateless** — explode each game into 2 player rows | none | Cassandra + Postgres `elo_history` |
| `opening_trends` | **windowed** — 5-min tumbling window, count by ECO | 5 min | Postgres `opening_trends` |
| `tournament_leaderboard` | **stateful** — running sum per `(tournament_id, player_id)` | 1 hour | Cassandra + Postgres `tournament_leaderboard` |
| `live_activity` | **two queries in one app** — 1-min throughput window + per-game running count | 2 min / 10 min | Postgres `live_activity` + `game_lengths` |
| `live_featured` | **stateless** — one event → one upsert | none | Postgres `featured_players` |

> "Each job is one Spark application capped at one core and 512 MB executor
> memory. Five apps on a 6-core / 4 GB cluster — fits with one core to spare
> for ad-hoc batch."
>
> "A subtle point on the ELO tracker: Lichess PGN already contains
> `WhiteRatingDiff` and `BlackRatingDiff`. The delta is in the message, so
> the job is stateless — no state store, no watermark needed. That's a
> design decision driven by data inspection."

### Q&A

**Q: What's a watermark?**
> A bound on how late an event can arrive before it's dropped. Without
> watermarks, a stateful aggregation accumulates state forever — eventually
> OOMs. With a 5-min watermark on opening trends, events older than 5 min
> after the current max event time are dropped, and the window state can be
> evicted.

**Q: What does `outputMode("update")` mean?**
> Three output modes: `append` (only new rows), `complete` (entire aggregated
> state every batch), `update` (only changed rows). For tournament leaderboard
> we use `update` because we only care about the players whose totals changed
> this batch — much cheaper than `complete`, which would re-emit the entire
> leaderboard every 30 seconds.

**Q: Why `foreachBatch` instead of `df.writeStream.format("jdbc")`?**
> Two reasons. (1) JDBC streaming sink doesn't support upserts — only
> appends. We need `ON CONFLICT DO UPDATE` for windowed jobs whose values
> change. (2) `foreachBatch` lets us write to multiple sinks in one batch —
> for example, Cassandra and Postgres simultaneously.

**Q: What's the trigger interval?**
> 10 seconds for `elo_tracker`, 30 seconds for the others. It's a
> latency/throughput trade-off — shorter intervals mean lower latency but
> more overhead per batch.

**Q: How does Spark handle a job crashing?**
> Each job's checkpoint directory contains its Kafka offsets and state store
> snapshots. On restart, Spark replays from the last committed offset and
> rebuilds state. Exactly-once semantics in the read path; we get
> at-least-once on the sink because Postgres upserts are idempotent.

**Q: Why is the ELO tracker stateless?**
> Because Lichess pre-computed the rating change in the PGN. If we were
> computing ELO from scratch from game outcomes, we'd need state — running
> ELO per player. We discovered the `WhiteRatingDiff` field during data
> inspection and simplified accordingly.

---

## Part 6 — Batch processing (2 min)

> "Three Spark MLlib batch jobs. They read the same `game-results` topic
> from Kafka — but in *batch* mode, with `endingOffsets=latest` — meaning
> they snapshot the whole topic at submit time. Kafka serves as both stream
> and batch source. Same data, two read patterns."
>
> "Job 1 — K-Means opening clustering. We aggregate per-ECO features (game
> count, white-win-rate, black-win-rate, draw rate, average ELOs, average
> time control), standardize with StandardScaler, run K-Means with k=5.
> The 5 cluster labels are assigned post-hoc by ranking centroids on
> white-win-rate."
>
> "Job 2 — Player styles. Aggregate per-player win rate, loss rate, ELO
> volatility, opening diversity. Computes aggression index as win rate
> minus loss rate. Honest note: the original spec wanted board-level metrics
> like captures and pawn advances — those need move-level data we don't
> ingest at scale, so we use this game-level proxy."
>
> "Job 3 — ALS recommender. Spark MLlib ALS with implicit feedback. Builds
> a sparse `(player, ECO, play_count)` matrix, factorizes into 20-dimensional
> latent vectors per player and per opening, then computes top-10
> recommendations per player. Writes JSONB to Postgres `als_vectors`."

### Q&A

**Q: Why batch and stream from the same Kafka topic?**
> It demonstrates Kafka's dual role. Streaming jobs care about latency —
> they want events as soon as possible. Batch jobs care about global views
> — they need the full topic. Same data, same source, different read mode.
> No extra ingestion layer needed.

**Q: Why K=5 for K-Means?**
> Empirically. We didn't sweep K with silhouette score — for an academic
> deliverable, 5 produced clean, human-readable separations: Sharp Tactical,
> Solid Mainline, Drawish Positional, Black-Friendly, Offbeat. A production
> system would tune K via cross-validation.

**Q: What's ALS and why implicit feedback?**
> Alternating Least Squares is matrix factorization. We have a sparse
> `players × openings` matrix where most cells are zero. ALS decomposes it
> into two low-rank matrices — 20 dimensions per row, 20 per column — such
> that their product reconstructs the original. The recommendations come
> from the product of player and opening vectors. Implicit feedback means
> we treat play counts as *confidence* (a player who played the Sicilian
> 100 times really likes it), not ratings — there's no explicit dislike
> signal in our data.

**Q: How long does ALS take?**
> About 45 seconds on our dataset of 14 000 interactions and 3 800 players.
> Spark MLlib parallelizes the factorization across all available cores.

**Q: What's deferred? Why?**
> Move NLP — would analyze game commentary text with Spark NLP. The standard
> rated dump has no commentary; we'd need to ingest the Lichess annotated
> dataset from Kaggle, which is a separate source. Scope decision: focus on
> the four jobs we could actually validate end-to-end.

**Q: How do you schedule batch jobs in production?**
> We use manual `spark-submit` for the demo. Production would use Airflow
> DAGs with daily/hourly schedules and proper dependency management.

---

## Part 7 — Storage (2 min)

> "Polyglot persistence. Same logical data lives in different stores tuned
> for different access patterns."
>
> "Cassandra holds time-series — `elo_history` and `tournament_standings`.
> Write-heavy, append-mostly. PK design `(player_id, recorded_at)` with
> `CLUSTERING ORDER BY recorded_at DESC` means 'latest ELO for player X' is
> a single sequential read. Cassandra natively upserts on PK collision."
>
> "PostgreSQL is the analytical / presentation layer. Eight tables. Grafana
> reads from it (Grafana has a native Postgres datasource; Cassandra
> support is unsigned/community-only). We use Postgres for any query that
> needs JOINs across tables — ALS recommendations joined with cluster labels,
> for example."
>
> "Elasticsearch has a `chess_openings` index mapping defined. Currently
> scaffolded but not populated — would feed an opening-name fuzzy search
> in a future React UI."

### Q&A

**Q: Why both Cassandra and Postgres? Isn't that duplication?**
> Yes, intentional. Cassandra is the architectural source of truth for
> time-series — high write throughput, predictable latency, horizontal
> scaling on partition key. Postgres is a presentation cache. Cost is one
> extra `df.write` per batch. Benefit: Grafana works out of the box, and
> JOINs are trivial in SQL.

**Q: How big could Cassandra get?**
> Practically unbounded for time-series. Each partition (one per player_id)
> grows linearly with games played. Cassandra's compaction strategy keeps
> read latency stable. Postgres would be the constraint long before
> Cassandra is.

**Q: Why not just use Cassandra and skip Postgres?**
> Three reasons. (1) Grafana's Cassandra plugin is unsigned, adds ops
> friction. (2) Cassandra doesn't do JOINs — every analytical query that
> spans tables would need application-level join logic. (3) Postgres has
> rich SQL we actually use: `GROUP BY`, `JOIN`, window functions, JSONB.

**Q: What's JSONB?**
> Postgres's binary JSON type. We use it for `als_vectors.factors` — each
> row stores a JSON array of 10 recommendations. JSONB supports indexing
> and SQL operators like `factors->0->>'eco'` to extract the top
> recommendation in a query.

**Q: How does the per-second collision in PGN work?**
> Lichess PGN timestamps are per-second. A player can finish two bullet
> games in the same UTC second. Our original Postgres `elo_history` had
> `(player_id, recorded_at)` as PK, and JDBC insert rejected duplicates.
> Solution: surrogate `BIGSERIAL id` PK, with `(player_id, recorded_at)`
> as a non-unique index. Cassandra silently upserts so it's unaffected.
> The live stream emits millisecond timestamps, so this is a PGN-only
> concern.

---

## Part 8 — Visualization (2 min)

> "Two visualization tools, deliberately split by use case."
>
> "Grafana — operator view. Five dashboards backed by Postgres. Refreshes
> every 10-30 seconds. Time-series-first. ELO tracker, Opening trends,
> Tournament leaderboard, Batch insights, and a Chess Pipeline Overview."
>
> "Metabase — analyst view. Two dashboards. Live Stream shows the
> real-time data from Lichess TV — moves per minute, currently featured
> players, game length distribution. Cross-table dashboard does the JOINs
> Grafana can't do cleanly — like 'top aggressive players × their #1 ALS
> recommendation with cluster annotation'."
>
> "Both read the same Postgres database. The split is by user need: Grafana
> for monitoring, Metabase for exploration."

### Q&A

**Q: Why both? Aren't they redundant?**
> They have different strengths. Grafana's time-series rendering and
> templating variables are far better than Metabase's. Metabase's SQL
> editor and JOIN ergonomics are far better than Grafana's. Picking one
> would have meant accepting weakness on one side.

**Q: How does Metabase setup work?**
> Automated via the Metabase API. `metabase/setup.sh` reads admin
> credentials from `.env`, POSTs to `/api/setup` to create the admin user
> and add the ChessDB Postgres datasource. Idempotent — re-running detects
> existing setup and just verifies the datasource is present.

**Q: How would you add a new dashboard?**
> Grafana: write a JSON file in `visualization/grafana/provisioning/dashboards/`
> and Grafana picks it up on its next refresh. Metabase: build through the
> UI, then optionally export to JSON via Metabase's serialization for
> reproducibility.

---

## Part 9 — Live demo flow (5–7 min)

This is the actual screen-by-screen sequence to perform.

### Step A — Show the running infrastructure

Terminal:
```powershell
docker compose ps
```
> "Fourteen services, all healthy. Zookeeper plus Kafka are the message bus.
> Spark master plus two workers are the cluster. Cassandra, Postgres,
> Elasticsearch — polyglot storage. Grafana, Metabase, Kibana — visualization
> and search UIs."

### Step B — Show Kafka topics

Browser: http://localhost:8080 → cluster `chess-cluster` → Topics.

> "Five topics. Click `game-results` → Messages. This is one PGN-derived game
> event — `Ayman22` vs `daamien`, ECO B10, played at this 2016 timestamp."

### Step C — Show Spark UI

Browser: http://localhost:8090.

> "Five streaming applications, each capped at one core. Both 'sides' of the
> pipeline running concurrently — three jobs consuming PGN-replay topics and
> two consuming live TV topics."

### Step D — Start the live producer

Side terminal:
```powershell
.\.venv\Scripts\python.exe -m ingestion.stream_producer.main --bootstrap localhost:29092 --max-reconnects -1
```
> "This holds a connection to `lichess.org/api/tv/feed`. Every move played
> on Lichess TV right now lands in our `game-moves` Kafka topic within a
> couple of seconds."

Within 5 seconds you'll see `featured -> game_id=... players=(...)` lines.

### Step E — Walk the Grafana dashboards

Browser: http://localhost:3000 → admin/admin123 → Dashboards → Chess Pipeline folder.

**ELO Tracker** → pick a player from the dropdown.
> "This is end-to-end streaming on historical data. The 2016 PGN games were
> replayed through Kafka; the ELO tracker streaming job picked them up;
> they landed in Postgres; Grafana renders the time series. Timestamps
> reflect when the games were played in 2016, not when we ingested them
> — that's the difference between *stream processing* and *live data*."

**Opening Trends** → show the stacked time series.
> "Five-minute tumbling windows. The top opening was `A00 Hungarian Opening`
> with 536 games — mostly 1.b3 sidelines in 2016."

**Tournament Leaderboard** → pick a tournament.
> "Top-20 standings. Notice `Fanatist` with 11 points in 11 games — perfect
> record. Exactly the kind of pattern bot-detection would flag if we'd
> built it."

**Batch Insights** → cluster pie, aggression histogram, ALS recommendations.
> "Pie chart shows K-Means output — 287 ECOs into 5 clusters. Aggression
> histogram is bell-shaped around zero because Lichess matches players by
> ELO. Pick a player from the dropdown — these are ALS top-10 personalized
> opening recommendations."

### Step F — Switch to Metabase for live data

Browser: http://localhost:3003 → admin@chess.local / Chess123! → Browse Data → ChessDB.

> "Grafana shows the historical replay. Metabase shows the live half — the
> Lichess TV data we're ingesting right now."

Query: `SELECT featured_at, white_player, white_rating, black_player, black_rating FROM featured_players ORDER BY featured_at DESC LIMIT 10`

> "These are the players currently on Lichess TV. Notice the ratings —
> 2700-3100, super-GM level. The live producer captures them within
> seconds of Lichess featuring them on TV."

### Step G — Storage inspection

Terminal:
```powershell
docker exec -it big-data-project-lichess-cassandra-1 cqlsh
```
```cql
USE chess;
SELECT player_id, recorded_at, elo, delta FROM elo_history LIMIT 5;
```
> "Cassandra holds the time-series. PK design clusters by `recorded_at DESC`
> so 'latest ELO' is a sequential read."

```cql
EXIT;
```

> "Stop the live producer in the other terminal with Ctrl+C."

---

## Part 10 — Honest about what's deferred (1 min)

> "Three pieces are intentionally deferred, all for the same reason — they
> need data sources we didn't ingest."
>
> "Bot detection — the streaming job that flags cheaters. Needs Stockfish
> engine match rates, which means dockerizing Stockfish and adding an
> enrichment producer. Scope decision."
>
> "Move NLP — would analyze game commentary text. The standard rated PGN
> dump has no commentary. Would need to ingest the Lichess annotated dataset
> separately."
>
> "React opening explorer — a player-facing front-end. We chose to invest
> the time in Metabase analytics instead, which gives the same analytical
> reach with less custom code."

### Q&A

**Q: How would you add bot detection given more time?**
> Three steps. (1) Dockerize Stockfish with a small Python wrapper exposing
> a UCI interface. (2) Add an enrichment producer that reads `game-moves`,
> sends each position to Stockfish, computes engine match rate per player.
> (3) Write a `bot_detection` stream job that aggregates engine match rate
> per `(player_id, 1-hour window)`. The `bot_scores` Cassandra table is
> already in the schema.

**Q: Why didn't you build the React UI?**
> Two reasons. (1) Time budget — Metabase gives us analytical
> visualizations with zero front-end code. (2) The course evaluates the
> data engineering layers; a custom UI is presentation polish, not
> architectural contribution.

---

## Closing (1 min)

> "To summarize what we built: a 14-service big data pipeline that ingests
> from a batch PGN source and a real-time API, processes events through
> Kafka with five concurrent Spark Structured Streaming jobs and three
> MLlib batch jobs, stores results in a polyglot Cassandra + Postgres +
> Elasticsearch layer, and surfaces insights through Grafana for operators
> and Metabase for analysts."
>
> "The architecture cleanly separates concerns at each layer. Each layer
> could be replaced — Kafka with Kinesis, Cassandra with ScyllaDB, Spark
> with Flink — without touching the others. That's the value of the
> message-bus pattern."
>
> "Questions?"

---

## Catch-all Q&A — questions that span multiple parts

**Q: What was the hardest problem you faced?**
> Three candidates. (1) The Bitnami Spark image runs as UID 1001 with no
> `/etc/passwd` entry, which causes Hadoop's JAAS authentication to
> NullPointerException on `getpwuid`. Fixed by appending a `spark:x:1001`
> line in the Dockerfile. (2) Per-second `(player_id, recorded_at)`
> collisions in Postgres `elo_history` — fixed with a surrogate
> `BIGSERIAL` PK. (3) Spark's JDBC writer doesn't support JSONB directly,
> so our ALS job writes to a staging table then promotes via
> `INSERT … SELECT factors::jsonb` over psycopg2.

**Q: What surprised you about the data?**
> Two things. (1) `WhiteRatingDiff` is already in the PGN — saved us from
> implementing stateful streaming for ELO tracking. (2) About 17% of games
> in the sample are tournament games, much higher than I'd guessed.

**Q: How long did this take?**
> Built incrementally over multiple sessions. Phase 0 (scaffolding) was
> the heaviest because docker-compose orchestration is finicky. After that
> each phase was 1-2 sessions: PGN producer + ELO tracker first
> (the "vertical slice"), then opening trends, then tournament leaderboard,
> then batch ML jobs, then live stream + Metabase last.

**Q: What would you do differently?**
> Two things. (1) Mount the Spark checkpoint directories on a Docker
> volume from day one — currently they're in `/tmp` so they reset on
> container restart, which complicates demos. (2) Pick the executor memory
> budget earlier — we hit a resource starvation issue when going from 3
> to 5 streaming jobs because each was claiming 1 GB by default.

**Q: How do the streaming jobs know when to commit?**
> Two-phase. Each Spark micro-batch (1) reads from Kafka producing a
> bounded DataFrame, (2) calls `foreachBatch` to write to the sink, (3)
> on successful sink write, commits the Kafka offsets to the checkpoint
> directory. If the sink write fails, the batch is retried — exactly-once
> on the read path, at-least-once on the sink. Idempotent sinks (upserts)
> make this safe.

**Q: Is this production-ready?**
> No, and we shouldn't pretend it is. It's an academic deliverable that
> demonstrates the architecture. For production we'd need: (1) Airflow
> for batch scheduling, (2) proper monitoring with Prometheus/Alertmanager,
> (3) authentication on Kafka and Postgres, (4) replication factor > 1
> everywhere, (5) Kafka schema registry to lock down message contracts,
> (6) integration tests for each streaming job, (7) actual GPU acceleration
> for ALS at scale.

**Q: What's the throughput of the whole pipeline?**
> End-to-end measured: PGN producer at 800-1000 msg/s, streaming jobs
> consume in sub-second batches, Grafana panel updates within 10 seconds
> of an event arriving in Kafka. Bottleneck is the throttled producer.

**Q: What if I asked you to make one change right now to improve it?**
> Persist the streaming checkpoints to a Docker volume. Currently if any
> Spark container restarts, the streaming jobs lose their Kafka offsets
> and restart from `latest` — meaning they'll skip whatever Kafka events
> arrived during the downtime. A named volume on `/tmp/checkpoints` would
> fix it with one line of `docker-compose.yml`.

---

## Cheat sheet — things to memorize

- **14 docker services**
- **5 Kafka topics**: `game-results`, `tourn-events`, `game-moves`, `player-events` (plus `openings` reserved for K-Means feedback loop)
- **5 streaming jobs**: ELO tracker, opening trends, tournament leaderboard, live activity, live featured
- **3 batch jobs**: K-Means (k=5), player styles, ALS (rank=20, implicit)
- **3 storage layers**: Cassandra (time-series), Postgres (analytical), Elasticsearch (scaffolded)
- **2 viz tools**: Grafana (operator), Metabase (analyst)
- **Sample data**: Feb 2016 standard rated PGN, ~867 MB compressed, ~30k events ingested in our demo
- **Resource budget**: 6 cores / 4 GB cluster, 5 streams × 1 core × 512 MB each
- **K-Means K**: 5 clusters, labels: Sharp Tactical, Solid Mainline, Drawish Positional, Black-Friendly, Offbeat
- **ALS hyperparameters**: rank=20, maxIter=10, regParam=0.1, implicitPrefs=True
- **Watermarks**: opening trends 5 min, tournament leaderboard 1 hour, live activity 2 min / 10 min

---

## If she pushes for an honest weakness

Don't dodge. The two most defensible honest weaknesses are:

1. **The live stream and the historical stream don't merge.** ELO tracker only sees PGN-replay data; live TV moves go to separate tables. With more time we'd extend the live producer to emit synthesized `game-results` events on `t=finish` so both halves feed the same downstream jobs.

2. **The K-Means cluster labels are post-hoc and subjective.** We named the clusters by eyeballing the cluster centers; a proper analysis would validate with silhouette score and chess-domain expert review.

Honest weaknesses scored more highly in our last presentation than dodged ones. Lead with confidence, then admit where the design is open.

Good luck.
