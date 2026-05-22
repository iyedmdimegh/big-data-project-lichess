# Demo Guide — On-Stage Cheat Sheet

Scannable step-by-step for the live demo. Print this or have it open on a second screen. **What to click, what to say, what to expect on screen.**

For the full runbook with Q&A and recovery, see [DEMO.md](DEMO.md). For the talk script, [PRESENTATION.md](PRESENTATION.md).

---

## 10 minutes before the demo

Open these 7 tabs in your browser:

1. http://localhost:8080 — **Kafka UI**
2. http://localhost:8090 — **Spark Master UI**
3. http://localhost:3000 — **Grafana** (auto-login as `admin` / `admin123`)
4. http://localhost:3003 — **Metabase #1** (`admin@chess.local` / `Chess123!`)
5. http://localhost:3004 — **Metabase #2** (`admin@chess-cassandra.local` / `Chess123!`)
6. http://localhost:8081/ui/ — **Presto UI**
7. http://localhost:5601 — **Kibana** (only if asked about Elasticsearch)

Open these 3 terminals in this order:

- **T1** — repo root, venv activated. For the live producer.
- **T2** — repo root. For cqlsh / psql / kafka-console-consumer queries.
- **T3** — repo root. Emergency / recovery commands if anything dies.

Run the pre-demo checklist in T2:

```powershell
docker exec big-data-project-lichess-spark-master-1 sh -c "ps -ef | grep -v grep | grep -E 'elo_tracker.py|opening_trends.py|leaderboard.py|live_activity.py|live_featured.py' | wc -l"
```

**Expect 15.** If less, run the missing `submit.sh` scripts (see Recovery below).

Then verify data:

```powershell
docker exec big-data-project-lichess-postgres-1 psql -U chess -d chessdb -c "SELECT 'elo_history', COUNT(*) FROM elo_history UNION ALL SELECT 'opening_trends', COUNT(*) FROM opening_trends UNION ALL SELECT 'tournament_leaderboard', COUNT(*) FROM tournament_leaderboard UNION ALL SELECT 'opening_clusters', COUNT(*) FROM opening_clusters UNION ALL SELECT 'als_vectors', COUNT(*) FROM als_vectors;"
```

**Expect roughly:** elo_history 20000, opening_trends 2494, leaderboard 609, clusters 287, als 3782. If any are zero, see Recovery.

---

## The demo flow (15-20 min)

Total of 8 stages. Allot ~2 min per stage. Don't linger.

### Stage 1 — Infrastructure (1 min)

**Terminal T2:**
```powershell
docker compose ps --format "table {{.Service}}\t{{.Status}}"
```

**Say:** "Fourteen Docker services. Zookeeper + Kafka are the message bus. Spark master + 2 workers are the cluster. Cassandra + Postgres + Elasticsearch are polyglot storage. Grafana + 2 Metabase instances + Kibana are the visualization layer. One `docker compose up -d` brings everything online."

**Don't forget:** point out that all services have healthchecks.

---

### Stage 2 — Kafka topics (2 min)

**Browser: Kafka UI tab** (http://localhost:8080)

1. Click **chess-cluster** → **Topics**.
2. Point at the 5 topics.

**Say:** "Five topics — `game-results` from the batch PGN producer, `tourn-events` for tournament games specifically, then `game-moves` and `player-events` from the live producer. Keyed by entity ID so stateful aggregations never cross-shard."

3. Click `game-results` → **Messages** tab.
4. Click any message → show the JSON payload.

**Say:** "Here's one game event. Ayman22 vs daamien, ECO B10 Caro-Kann, played in 2016. This is the canonical schema all downstream jobs consume."

---

### Stage 3 — Spark cluster + 5 streams (1 min)

**Browser: Spark UI tab** (http://localhost:8090)

Point at the **Running Applications** section.

**Say:** "Five Spark Structured Streaming applications running concurrently. Each capped at one core and 512 MB executor memory because we have a 6-core / 4 GB cluster and need headroom. Three of them consume the PGN-replay topics — ELO tracker, opening trends, tournament leaderboard. Two consume the live TV topics — live_activity and live_featured. We'll see what they produce in the dashboards next."

---

### Stage 4 — Start the live producer (30 sec)

**Terminal T1:**
```powershell
.\.venv\Scripts\python.exe -m ingestion.stream_producer.main --bootstrap localhost:29092 --max-reconnects -1
```

You'll see `featured -> game_id=... players=(...)` within 5 seconds.

**Say:** "This holds an HTTP connection to `lichess.org/api/tv/feed`. Every move played on Lichess TV right now lands in our `game-moves` Kafka topic within seconds. Watch the player names — these are 2700-3100 rated, super-GM level. This is real live data, not a simulation. We'll leave this running for the rest of the demo."

**Leave running.** Don't Ctrl+C until the very end.

---

### Stage 5 — Grafana dashboards (5 min)

**Browser: Grafana tab** → Dashboards → **Chess Pipeline** folder.

#### 5a. ELO Tracker

1. Click **ELO Tracker** dashboard.
2. Pick any player from the **$player** dropdown (e.g., `microcommega`).

**Say:** "This is end-to-end streaming. PGN game → Kafka → stateless Spark job → both Cassandra and Postgres → Grafana auto-refreshes every 10 seconds. The timestamps are 2016 because that's when the games were *played* — streaming processing on replayed historical data. The pipeline is live; the event-time is historical."

**Don't skip:** explicitly explain stream-processing vs live-data distinction here. The professor may ask.

#### 5b. Opening Trends

1. Click **Opening Trends** dashboard.

**Say:** "This is the windowed pattern. 5-minute tumbling windows, group by ECO opening code. Watermark drops events older than 5 minutes after the current event time. The top opening was `A00 Hungarian Opening` — 536 games. In 2016 that was mostly 1.b3 sidelines."

#### 5c. Tournament Leaderboard

1. Click **Tournament Leaderboard** dashboard.
2. Pick any tournament from the dropdown.

**Say:** "Stateful aggregation pattern. Per `(tournament_id, player_id)`, running sum of points — 1 for a win, 0.5 for a draw. Watermark 1 hour because tournaments are short-lived. Output mode `update` so we only upsert players whose totals changed in this batch."

Point at `Fanatist` with 11/11.

**Say:** "Eleven points in eleven games. Perfect record. Exactly the pattern bot-detection would flag — which is the next layer we'd build."

#### 5d. Batch Insights

1. Click **Batch Insights** dashboard.

**Point at panels in order:**

- **Cluster pie:** "287 ECO codes clustered into 5 groups via K-Means. Drawish Positional is the biggest at 159 — that's all the rare openings that look similar statistically."
- **Aggression histogram:** "3782 players. Win-rate minus loss-rate. Bell-shaped around zero because ELO matchmaking forces balance. The tails are interesting — at the high end are perfect-record players, candidates for bot detection."
- **Player dropdown → Recommendations table:** Pick a player. "ALS collaborative filtering. Each player has unique top-10 ECO recommendations based on what similar players in the latent space have played. Spark MLlib, rank 20, implicit feedback because play count is a confidence signal, not a rating."

---

### Stage 6 — Metabase #1 for live data (2 min)

**Browser: Metabase #1 tab** (http://localhost:3003)

**Say:** "Grafana is the operator view — refresh every 10 seconds, time-series-first. Metabase is the analyst view — ad-hoc SQL with JOINs. Both read the same Postgres."

1. Click **+ New** → **SQL query** → pick **ChessDB**.
2. Paste:
   ```sql
   SELECT featured_at, white_player, white_rating, black_player, black_rating
   FROM featured_players
   ORDER BY featured_at DESC LIMIT 10;
   ```
3. Click **▶ Run**.

**Say:** "These are the players currently on Lichess TV — captured by the live producer we started a few minutes ago. The producer reads NDJSON, the live_featured stream upserts into Postgres, Metabase reads it. End-to-end live."

4. Run a cross-table JOIN query:
   ```sql
   SELECT ps.player_id, ROUND(ps.aggression_index::numeric, 3) AS aggr,
          (av.factors->0->>'eco') AS top_rec
   FROM player_styles ps
   JOIN als_vectors av USING (player_id)
   ORDER BY ps.aggression_index DESC LIMIT 10;
   ```

**Say:** "And this is what Metabase does that Grafana doesn't — JOINs across stream-job output and batch-job output. The aggressive players from the batch player_styles table, joined with their #1 ALS recommendation. Unified query layer in Postgres."

---

### Stage 7 — Metabase #2 + Presto + Cassandra (2 min) ⭐

**This is the strongest moment — the one that answers "is Cassandra actually used?"**

**Browser: Metabase #2 tab** (http://localhost:3004)

1. Click **+ New** → **SQL query** → pick **ChessCassandra**.
2. Paste:
   ```sql
   SELECT COUNT(*) AS rows FROM chess.elo_history;
   ```
3. Click **▶ Run**. Returns ~20000.

**Say:** "This is a separate Metabase instance — port 3004 — connected to PrestoDB, which has a Cassandra connector. The query you just saw went: Metabase → Presto JDBC driver → PrestoDB → Cassandra native protocol → Cassandra → back. Live read straight from Cassandra. This proves Cassandra has a real read path, not just write path."

4. Run a player-specific query:
   ```sql
   SELECT player_id, recorded_at, elo, delta
   FROM chess.elo_history
   WHERE player_id = 'microcommega'
   ORDER BY recorded_at DESC LIMIT 10;
   ```

**Say:** "Note the WHERE clause picks the partition key. This is the access pattern Cassandra is designed for — single-partition lookup. Postgres aggregates beat Cassandra at GROUP BY queries; Cassandra wins at this pattern."

**If asked why two Metabase instances:** "Isolation. If the Presto driver has any quirk we don't want to risk it breaking the main analytics Metabase. Separate metadata database, separate instance, separate failure domain."

---

### Stage 8 — Storage inspection via shell (2 min)

**Terminal T2:**

```powershell
docker exec -it big-data-project-lichess-cassandra-1 cqlsh
```

Inside cqlsh:
```cql
USE chess;
SELECT player_id, recorded_at, elo, delta FROM elo_history WHERE player_id = 'microcommega' LIMIT 5;
SELECT player_id, points, wins, draws, losses FROM tournament_standings WHERE tournament_id = 'oSyPykCM' LIMIT 5;
EXIT;
```

**Say:** "Same Cassandra data, queried natively this time via CQL. PK design partitions by `player_id` with clustering `recorded_at DESC` so the latest ELO is a single sequential read. No secondary index, no full table scan."

```powershell
docker exec -it big-data-project-lichess-postgres-1 psql -U chess -d chessdb
```

Inside psql:
```sql
SELECT player_id, jsonb_pretty(factors) FROM als_vectors LIMIT 1;
\q
```

**Say:** "And the ALS recommendations as JSONB — Postgres-specific feature. JDBC didn't support JSONB writes directly so the ALS batch job writes a staging table as text, then promotes with `::jsonb` cast through psycopg2."

---

### Stage 9 — Close

**Stop the live producer** (Ctrl+C in T1).

**Say:** "To summarize: dual ingestion, Kafka decouples five streaming jobs and three batch jobs, polyglot Cassandra-Postgres-Elasticsearch with deliberate write/read separation, and two visualization tools targeting different use cases. The architecture is the same one you'd deploy at production scale. Questions?"

---

## Things to absolutely not do

- **Don't run `docker compose down`** before or during the demo. Even briefly. State doesn't survive.
- **Don't restart any Spark container** during the demo. Streaming jobs die and need manual resubmit.
- **Don't change the Grafana time range** to "Last 7 days". The PGN data is from 2016 so the panel goes blank. The default is set correctly — leave it alone.
- **Don't open more than 2-3 Metabase questions simultaneously**. The Presto path is the slowest, can backlog.
- **Don't try to demo the dashboard on Metabase #2** for cross-table joins. Presto-on-Cassandra works for simple selects, not aggregate-heavy JOINs.

---

## Recovery (if something goes wrong)

### A streaming job died

```powershell
docker exec big-data-project-lichess-spark-master-1 sh -c "ps -ef | grep -v grep | grep -E 'tracker.py|trends.py|leaderboard.py|activity.py|featured.py'"
```

If less than 15 processes, restart missing ones. Each takes ~30s to come back up.

```powershell
bash stream_processing/elo_tracker/submit.sh
bash stream_processing/opening_trends/submit.sh
bash stream_processing/tournament_leaderboard/submit.sh
bash stream_processing/live_activity/submit.sh
bash stream_processing/live_featured/submit.sh
```

### Grafana panel shows "No data"

- **Most likely:** time range is wrong. Click the time picker, set to **"2016-01-31 21:30 to 2016-01-31 23:00 UTC"** (or use the dashboard defaults).
- **Less likely:** Postgres table is empty. Verify with the count query from pre-demo.

### Live producer hangs / shows no output

Stop with Ctrl+C, restart:
```powershell
.\.venv\Scripts\python.exe -m ingestion.stream_producer.main --bootstrap localhost:29092 --max-reconnects -1
```

Lichess sometimes has slow TV switches — wait 30 seconds.

### Metabase #2 says "no data" or timeout

Presto is slow on first query (JIT warmup). Run the query twice. If still failing, the Cassandra driver may need a refresh:

```powershell
docker restart big-data-project-lichess-trino-1
```

Wait 60 seconds before retrying. **DO NOT** do this for the regular Cassandra path — only for the Presto-Cassandra one if it breaks.

### Kafka topic returns no messages

Topic was wiped. Re-create:
```powershell
docker exec big-data-project-lichess-kafka-1 kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic game-results --partitions 3 --replication-factor 1
```

Then run the batch PGN producer to repopulate:
```powershell
.\.venv\Scripts\python.exe -m ingestion.batch_pgn_producer.main --pgn data/lichess_db_standard_rated_2016-02.pgn.zst --bootstrap localhost:29092 --rate 800 --max-games 5000
```

This takes ~10 seconds.

### Total panic — everything is broken

Don't `docker compose down`. Instead:

```powershell
docker compose restart spark-master spark-worker-1 spark-worker-2
```

Wait 60 seconds. Then resubmit all 5 streams. Postgres / Cassandra / Kafka data survives container restarts because they have named volumes.

---

## What to say if asked "what would you change?"

Three honest answers:

1. **"Wire bot detection."** It's the deferred-but-architecturally-ready piece. Stockfish container + enrichment producer + bot_detection stream job. The `bot_scores` Cassandra table is already in the schema. ~2-3 days of work.

2. **"Mount checkpoints on a Docker volume."** Currently `/tmp/checkpoints` is volatile — a Spark container restart loses Kafka offsets and the streams resume from `latest`. One-line compose fix.

3. **"Add Airflow."** Right now batch jobs run via manual `spark-submit`. Production would use Airflow DAGs with proper dependency management, retries, and scheduling.

---

## Memorize before walking in

| What | Number |
|---|---|
| Total Docker services | 14 |
| Kafka topics | 5 (the 4 we built + `__consumer_offsets`) |
| Streaming jobs running concurrently | 5 |
| Batch ML jobs | 3 (4th deferred — Move NLP) |
| Cluster size | 6 cores / 4 GB |
| Per-stream resources | 1 core / 512 MB |
| Postgres tables | 11 |
| Cassandra tables | 4 |
| K-Means K | 5 |
| ALS rank | 20 |
| Aggression histogram size | 3782 players |
| Top tournament leader | Fanatist, 11/11 perfect |
| Sample featured TV game | Heisenberg01 (3145) vs kakulia14 (2907) |

If you forget all the others, remember: **dual ingestion, polyglot storage, 5 streams + 3 batch jobs, Cassandra reads via Presto.** That's the spine of the story.

Good luck.
