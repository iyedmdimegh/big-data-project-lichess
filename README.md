# Chess Pipeline - Phase 0 Scaffolding

This repository is scaffolded for a 5-layer big data chess analytics platform:
1. Ingestion (Python + Kafka)
2. Stream processing (Spark Streaming)
3. Batch processing (Apache Spark)
4. Polyglot storage (Cassandra + PostgreSQL + Elasticsearch)
5. Visualization (Grafana + Metabase + React)

Phase 0 provides infrastructure only (Docker Compose + schemas + provisioning), with no application code yet.

## Services

| Service | Role | Local URL / Port |
|---|---|---|
| Zookeeper | Kafka coordination | `localhost:2181` |
| Kafka | Event bus (internal listener + host dev listener) | `localhost:29092` (host), `kafka:9092` (internal) |
| Kafka UI | Topic browser and consumer group inspector | http://localhost:8080 |
| Spark Master | Spark cluster coordinator and web UI | `spark://localhost:7077`, http://localhost:8090 |
| Spark Worker 1 | Spark execution node | http://localhost:8091 |
| Spark Worker 2 | Spark execution node | http://localhost:8092 |
| Cassandra | Low-latency move/event storage | `localhost:9042` |
| Cassandra Init | Applies `storage/cassandra/init.cql` after Cassandra is healthy | N/A |
| PostgreSQL | Relational analytics schema (`chessdb`) | `localhost:5434` |
| Metabase DB | Metadata DB used by Metabase | Internal only (`metabase-db:5432`) |
| Elasticsearch | Search/index storage for openings | http://localhost:9200 |
| Kibana | Elasticsearch inspection UI | http://localhost:5601 |
| Grafana | Operational dashboards | http://localhost:3000 |
| Metabase | BI / ad-hoc analytics UI | http://localhost:3003 |

## Quick Start

```bash
docker compose up -d
# wait ~60s for Cassandra and Elasticsearch to be ready
docker compose ps       # verify all services are healthy
```

## Verify

```bash
# Cassandra: keyspace and tables should exist
docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES;"
docker compose exec cassandra cqlsh -e "USE chess; DESCRIBE TABLES;"

# PostgreSQL: schema tables should exist
docker compose exec postgres psql -U chess -d chessdb -c "\dt"

# Elasticsearch: cluster health should be green or yellow
curl -s http://localhost:9200/_cluster/health?pretty

# Optional: create the chess_openings index with predefined mappings
curl -X PUT "http://localhost:9200/chess_openings" \
  -H "Content-Type: application/json" \
  --data-binary @storage/elasticsearch/mappings.json
```

## Notes

- All stateful services use named Docker volumes.
- All infrastructure services are attached to the `chess-net` bridge network.
- Secrets, ports, topic names, and version tags are externalized in `.env`.
