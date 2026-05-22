I'm presenting a big data engineering project to my software engineering professor tomorrow. Build me a complete presentation deck (~20-30 slides for a 15-20 minute talk). Aim for technical depth — this professor will probe hard during Q&A.

## The project

A chess analytics pipeline implementing the canonical 5-layer big data architecture, end-to-end on a single laptop via Docker Compose (14 services). Ingests both batch and stream data from Lichess (an open chess server with 100M+ games per month), processes through Kafka and Spark, persists to polyglot stores, visualizes through two BI tools.

## Layer 1 — Data sources

**Batch:** Lichess monthly PGN dumps from `database.lichess.org`. Our sample is the February 2016 standard rated dump — 867 MB zstandard-compressed, ~3-5 GB uncompressed, ~15-30 million games. Each game is a PGN block with headers (Event, Site, Date, White, Black, Result, ECO opening code, time control, ELOs, rating diffs, termination) plus a move list with optional inline `%eval` (Stockfish) and `%clk` (clock) annotations. Standard rated dumps have no `%eval`.

**Stream:** Lichess TV NDJSON API at `https://lichess.org/api/tv/feed`. Public, no auth. Persistent HTTP connection emitting one JSON event per line. Two event types: `t=featured` (Lichess switched to a new featured TV game — has player names, titles, ratings, starting FEN) and `t=fen` (a move was played — has new FEN, last move, clocks). Lichess TV always has live games, mostly 2700-3100 rated — super-GM level.

## Layer 2 — Ingestion

Two Python producers, both writing to Kafka with `acks=1, linger_ms=20, batch_size=65536`.

**Batch PGN producer** (`ingestion/batch_pgn_producer/`):
- `pgn_parser.py` — streaming generator using `zstandard.ZstdDecompressor.stream_reader` (memory stays constant regardless of file size) + `python-chess` for PGN parsing
- `main.py` — produces JSON events keyed by `game_id` to `game-results` topic; also extracts tournament URL from Event header via regex and emits a second message to `tourn-events` (about 17% of games are tournament games)
- Throttled with a token bucket to ~800-1000 msg/sec so streaming jobs can keep up

**Live stream producer** (`ingestion/stream_producer/`):
- Holds an HTTP stream via `requests.iter_lines()`, splits NDJSON events
- `featured` events → `player-events` topic with `ingested_at` timestamp
- `fen` events → `game-moves` topic, tagged with the currently-featured `game_id` (tracked across events)
- Exponential backoff up to 60s on disconnect, unlimited reconnects

## Layer 3 — Kafka (message bus)

Five topics:

| Topic | Partitions | Key | Producer | Consumers |
|---|---|---|---|---|
| `game-results` | 3 | game_id | batch PGN | elo_tracker, opening_trends, all 3 batch jobs |
| `tourn-events` | 3 | tournament_id | batch PGN (tournament games only) | tournament_leaderboard |
| `game-moves` | 6 | game_id | live stream | live_activity |
| `player-events` | 3 | game_id | live stream | live_featured |

`game-moves` gets more partitions because move events are ~30× higher volume than game events. Keys chosen so stateful aggregations grouping by that key never cross-shard.

Kafka exposes two listeners: `kafka:9092` (in-cluster for Spark) and `localhost:29092` (host-side for the Python producers).

## Layer 4 — Processing (Spark Structured Streaming + MLlib)

Cluster: Spark master + 2 workers × 3 cores × 2 GB = **6 cores / 4 GB total**. Custom Bitnami Spark 3.5 image with numpy, kafka-python, python-chess, cassandra-driver, psycopg2, plus JARs for the Kafka, Cassandra, and Postgres JDBC connectors baked in.

**5 streaming jobs, each capped at 1 core / 512 MB:**

1. **`elo_tracker`** — stateless. Reads `game-results`, explodes each game into 2 player rows `(player_id, recorded_at, elo, delta)`. Dual-sinks to Cassandra `chess.elo_history` AND Postgres `elo_history`. Stateless because Lichess PGN already contains `WhiteRatingDiff` / `BlackRatingDiff` — the delta is in the message, no state store needed. Trigger 10s.

2. **`opening_trends`** — windowed. Reads `game-results`, 5-min tumbling window on event-time `played_at` with 5-min watermark. `groupBy(window, eco).agg(count, ...)`. Output mode `update` so only changed windows emit. Sink: Postgres `opening_trends` via psycopg2 `execute_values + ON CONFLICT DO UPDATE` (JDBC streaming sink doesn't support upserts).

3. **`tournament_leaderboard`** — stateful aggregation. Reads `tourn-events`, explodes into 2 player rows with score contribution (1.0 / 0.5 / 0). `groupBy(tournament_id, player_id).agg(sum(score), count, sum(wins), sum(draws), sum(losses))`. Watermark 1 hour. Output mode `update`. Dual-sink to Cassandra `chess.tournament_standings` and Postgres `tournament_leaderboard`.

4. **`live_activity`** — **two streaming queries in one Spark application** (uses `spark.streams.awaitAnyTermination()`). Both consume `game-moves` from the same parsed DataFrame:
   - Query A: 1-min tumbling throughput window (2-min watermark) → moves_count, distinct active_games → Postgres `live_activity`
   - Query B: `groupBy(game_id)` running aggregation (10-min watermark) → move_count, first_seen, last_seen → Postgres `game_lengths`

5. **`live_featured`** — stateless. Reads `player-events`, filters `event_type == 'game_featured'`, projects + upserts to Postgres `featured_players`.

**3 batch jobs, read Kafka in batch mode (`spark.read.format("kafka")` with `endingOffsets=latest`):**

1. **`kmeans_openings`** — MLlib `KMeans(k=5)` over standardized per-ECO features (game count, white-win-rate, black-win-rate, draw rate, average ELOs, average time control). Centroid un-scaling + post-hoc labels by ranking on white-win-rate → "Sharp Tactical", "Solid Mainline", "Drawish Positional", "Black-Friendly", "Offbeat". Output: Postgres `opening_clusters` for all 287 ECOs.

2. **`player_styles`** — Aggregates 3782 players across both colors: games, win_rate, loss_rate, draw_rate, rating_volatility (stddev of rating_diff), opening_diversity (distinct ECO count), tournament_share. Derived `aggression_index = win_rate − loss_rate` ∈ [−1, +1]. Honest note: original spec wanted move-level metrics (captures, sacrifices, pawn advances) — we don't ingest moves at scale, so used this game-level proxy.

3. **`als_recommender`** — Spark MLlib `ALS(rank=20, maxIter=10, regParam=0.1, implicitPrefs=True)`. Builds sparse `(player_idx, eco_idx, plays)` matrix from `game-results`. Filters: ≥5 games per player, ≥5 games per ECO. `model.recommendForAllUsers(10)` produces top-10 ECO recommendations per player. Serialized as JSON array. Written to Postgres `als_vectors_stage` then promoted via `INSERT … SELECT factors::jsonb` to `als_vectors(player_id, factors JSONB, updated_at)` (Spark JDBC can't write JSONB directly).

## Layer 5 — Storage (polyglot)

**Apache Cassandra 4.1** — time-series source of truth.
- Keyspace `chess`, SimpleStrategy RF=1
- Tables: `elo_history(player_id, recorded_at, elo, delta)` PK `(player_id, recorded_at)` clustered DESC; `tournament_standings(tournament_id, player_id, points, games_played, wins, draws, losses, last_updated)` PK `(tournament_id, player_id)`; `game_moves` (schema only, not populated); `bot_scores` (reserved for deferred bot detection)
- Partition design: PK partitions by entity so "latest ELO for X" is a single-partition sequential read

**PostgreSQL 16** — analytical / presentation layer. 11 tables. Dual-sinked by streaming jobs.
- Owned by streams: `elo_history`, `opening_trends`, `tournament_leaderboard`, `live_activity`, `game_lengths`, `featured_players`
- Owned by batch: `opening_clusters`, `player_styles`, `als_vectors`
- Scaffolded: `players`, `openings`
- `elo_history` PK was originally `(player_id, recorded_at)` but PGN per-second timestamps caused JDBC INSERT collisions → switched to `BIGSERIAL id` PK with `(player_id, recorded_at DESC)` as a secondary index

**Why dual-sink Cassandra + Postgres** (the central architecture decision): Cassandra is the canonical write-side store (write-heavy, scales linearly, partitioned by entity). Postgres is the read-side (Grafana has a native Postgres datasource, Cassandra plugin is unsigned; JOINs and JSONB only work in Postgres; aggregate queries hate Cassandra). For the demo dataset (~1000 msg/sec, 30k events), Postgres alone could handle everything — Cassandra is over-engineered now but architecturally faithful and required for the deferred bot detection job at production scale.

**Elasticsearch 8.13** — index `chess_openings` mapping defined but not currently populated. Reserved for K-Means cluster labels and future fuzzy opening-name search.

## Layer 6 — Visualization

**Grafana 10.4** (port 3000, admin/admin123) — operator view. 5 dashboards provisioned from JSON: Chess Pipeline Overview, ELO Tracker, Opening Trends, Tournament Leaderboard, Batch Insights. Native Postgres datasource. Time-series-first. Default time range `2016-01-31 21:30-23:00 UTC` (the PGN sample's played time).

**Metabase #1** v0.49 (port 3003, admin@chess.local / Chess123!) — analyst view. Reads same Postgres database. Two dashboards: Live Stream (moves/min, active games, featured-players table, rating histogram, game-length histogram) and Cross-table analytics (joins across `player_styles`, `als_vectors`, `opening_clusters`, `opening_trends` — queries Grafana can't do cleanly). Automated bootstrap via `metabase/setup.sh` hitting `/api/setup`.

**Metabase #2 + PrestoDB** (port 3004, admin@chess-cassandra.local / Chess123!) — **the answer to "Cassandra is a write-only sink".** A separate Metabase instance with its own metadata Postgres connected to **PrestoDB 0.286** (a distributed SQL engine) which has a Cassandra connector. Chain: Metabase → Presto JDBC → Presto → Cassandra native protocol → Cassandra. Proven working: `SELECT COUNT(*) FROM chess.elo_history` returns 19998 directly from Cassandra. Why PrestoDB and not Trino? Metabase's bundled `presto-jdbc` driver is the legacy Facebook Presto client and gets HTTP 401 from Trino 350+ due to a protocol split. PrestoDB 0.286 speaks the same protocol as the driver.

**Kibana 8.13** (port 5601) — connected to Elasticsearch for index inspection.

## Cross-cutting concerns

- **Networking:** custom Docker bridge `chess-net`. Inter-container uses service hostnames (`kafka:9092`). Two external listeners exposed: Kafka 29092 (host-side producers), Postgres 5434 (host-side psql).
- **Configuration:** `.env` holds all ports, versions, credentials. Compose uses `${VAR}` interpolation.
- **Resources:** total budget ~12 cores / 10 GB requested, fits on a 4-core / 8 GB laptop because most services idle.
- **Observability:** Spark UI (8090), Kafka UI (8080), Grafana, container logs. No Prometheus.
- **Security:** academic deployment, no auth on most services, all bound to localhost.

## Issues hit and solved (real engineering moments — these are talk-worthy)

1. **JAAS NPE on getpwuid** — Bitnami Spark image runs as UID 1001 with no `/etc/passwd` entry. Hadoop's `UnixLoginModule` calls `getpwuid(1001)`, gets null, throws `KerberosAuthException: NullPointerException: invalid null input: name`. Fixed by appending `spark:x:1001:0:Spark user:/tmp:/bin/bash` in the Dockerfile.

2. **`spark.jars.ivy` is `?/.ivy2`** — same no-home-directory issue. `~/.ivy2` expands to `?/.ivy2`, breaks Ivy. Every submit script now sets `--conf spark.jars.ivy=/tmp/.ivy2`.

3. **Per-second PK collision** — PGN UTC timestamps are per-second; one player can finish two bullet games in the same second. JDBC INSERT rejected duplicates. Fixed: surrogate `BIGSERIAL id` PK, `(player_id, recorded_at DESC)` as a secondary index.

4. **Spark JDBC ≠ JSONB** — ALS recommendation array couldn't be written to Postgres JSONB directly. Solution: write to a staging table as text, then `INSERT … SELECT factors::jsonb` via psycopg2 in the same job.

5. **Resource starvation with 5 streams** — each Spark app defaulted to 1 GB executor memory. 5 × 1 GB > 4 GB worker memory → 5th stream stuck in WAITING state. Fix: `--conf spark.executor.memory=512m` in every submit script. 5 × 512 MB = 2.5 GB, fits with headroom.

6. **`numpy` missing on Spark workers** — MLlib K-Means/ALS need numpy on every executor. Image fix + runtime `pip install` on all three containers.

7. **Trino 444 vs Metabase's legacy Presto driver** — set up Trino as the SQL engine over Cassandra, got `Authentication failed: Unauthorized` on every Metabase query. Root cause: Metabase's `presto-jdbc` driver bundles `com.facebook.presto.jdbc.PrestoDriver` (the original Facebook Presto client). The Trino fork split in 2020 and Trino 350+ rejects the legacy protocol with HTTP 401. Fixed by swapping the image from `trinodb/trino:444` to `prestodb/presto:0.286`. Same SQL, same Cassandra connector, compatible protocol.

8. **Cassandra DCAwarePolicy NPE in Trino** — Trino's Cassandra connector required `cassandra.load-policy.dc-aware.local-dc` to be set explicitly even with `LOCAL_ONE` consistency. Added it to `cassandra.properties` as `datacenter1` (Cassandra's default DC name).

## Numbers / proof points (for slides showing "this actually works")

- 14 Docker services running on `chess-net`
- 5 Kafka topics with ~30k accumulated messages on `game-results`
- 5 streaming jobs running concurrently, each 1 core / 512 MB
- 3 batch jobs runnable on demand
- Postgres: `elo_history` 20000 rows, `opening_trends` 2494, `tournament_leaderboard` 609 (5 tournaments, 264 players), `opening_clusters` 287, `player_styles` 3782, `als_vectors` 3782, live tables 16+ activity windows / 12+ featured games per run
- Cassandra: `elo_history` 19998 (2 silent upserts on per-second collisions), `tournament_standings` 609
- Producer throughput: 800-1000 msg/sec throttled, 5000+ unthrottled
- Top K-Means cluster: Drawish Positional with 159 ECOs out of 287
- Sample ALS output: `IguanaAstronaut → C00, B01, A00`
- Live featured game captured: `Heisenberg01 (3145) vs kakulia14 (2907)` — super-GM TV-feed
- Top tournament leader: `Fanatist` with 11 points in 11 games — perfect record
- End-to-end latency: ~10s from event in Kafka to Grafana panel refresh

## Deferred — honest about what's NOT built (separate slide near the end)

| Component | Why deferred |
|---|---|
| Stockfish enrichment + bot detection | Needs UCI engine container; standard PGN has no `%eval`; reserved `bot_scores` Cassandra table waiting |
| Move NLP | Standard rated PGN has no commentary; would need Lichess annotated dataset as a third source |
| React + FastAPI UI | Replaced with Metabase #1 — same analytical reach, zero front-end code |
| K-Means → openings Kafka feedback loop | One extra `df.write` away; not built for time |
| Airflow orchestration | Manual `spark-submit` sufficient for academic scope |
| Streaming checkpoint persistence on volume | `/tmp/checkpoints` is volatile across container restarts; one-line compose fix |
| Schema registry (Avro) | JSON schemas documented in code is sufficient |
| Authentication / mTLS / RBAC | Single-host academic deployment, services bound to localhost |

## The narrative arc the slides should follow

1. The problem (Lichess scale, dual ingestion patterns)
2. The 5-layer architecture (overview diagram)
3. Data sources (concrete examples of PGN block + NDJSON events)
4. Ingestion (two producers, side-by-side)
5. Kafka topic design (table + key choice rationale)
6. Stream processing (each job gets a slide; the patterns matter more than the names — stateless, windowed, stateful, multi-query)
7. Batch ML processing (K-Means, player styles, ALS — what each does in one sentence)
8. Storage (the polyglot decision and Cassandra/Postgres CQRS-style split)
9. Visualization (Grafana + 2 Metabase instances + the Presto chain that closes the Cassandra read path)
10. Engineering challenges (pick 3-4 of the issues above for a "war stories" slide)
11. Numbers slide (proof it works)
12. Deferred work (honest, framed as "what scaling would look like")
13. Closing — questions

## Tone and audience

- Audience: CS / software engineering professor. Knows distributed systems and databases. Will probe on trade-offs.
- Tone: technical, confident, honest. Lead with architectural decisions, not "we built X". Acknowledge weaknesses as design choices, not failures.
- Don't oversell. The professor will catch hand-waving immediately.
- Use diagrams where they're clearer than prose: the 5-layer stack, the streaming-job pattern, the data-flow trace of one event, the Cassandra/Postgres dual-sink.

## Output

Build the complete deck. One concept per slide, max 5-7 bullet points each. Include code/config/SQL snippets where they're load-bearing (≤ 6 lines). Make sure the closing slide hands off to "Questions?". Don't ask me clarifying questions — make the call where there's ambiguity.
