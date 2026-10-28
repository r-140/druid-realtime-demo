# Druid Real-time Platform Lab

A hands-on lab for learning Apache Druid and operating a real-time OLAP platform. The domain is sales and refund events from a retail chain. Python + Apache Kafka + Apache Druid + PostgreSQL + ZooKeeper, with optional Prometheus.

The goal is to explain an event's journey to a SQL query, test delivery guarantees, and recover ingestion after a failure. The project includes a local environment and reproducible exercises. It is not a production deployment.

## Implemented features

- Docker Compose with separate Druid roles and persistent volumes.
- A Kafka topic with three partitions and an idempotent producer.
- A Python CLI for generation, publishing, supervisor APIs, SQL, reconciliation, batch rebuilds, and compaction.
- A generator for late events, business duplicates, malformed JSON, and schema v2.
- A raw datasource, optional minute-level rollup, and a separate late-event rejection policy.
- Manifests with expected record counts, distinct event counts, and monetary totals.
- SQL examples, a latency benchmark, an application-level metrics probe, and a Prometheus scrape configuration.
- Step-by-step [labs](docs/LABS.md), [runbooks](docs/RUNBOOKS.md), and [architecture and limitations](docs/ARCHITECTURE.md).

## Prerequisites

Linux/macOS/WSL2, Docker Engine or Docker Desktop with Compose v2, Python 3.11+, and internet access to download images and dependencies. Run the commands below in bash from the project root. `make` is optional.

For the full configuration, plan to allocate 12–16 GiB of RAM to Docker and have around 20 GiB of free disk space. This is an estimate, not a measured minimum requirement. On a smaller machine, start with only the `orders_raw` supervisor. MiddleManager has four task slots so that reading and publishing tasks from two supervisors can coexist temporarily. Run rebuilds and compaction separately from active streaming workloads.

Versions are deliberately pinned: Druid 35.0.0, Kafka 4.0.2, Postgres 16.8, ZooKeeper 3.9.3, and Prometheus 3.2.1. This is a reproducible learning baseline, not a claim that these are the latest releases. The configuration was checked against the official Druid Docker quickstart and ingestion documentation; see docs/SOURCES.md.

## First run

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v

docker compose config --quiet
docker compose up -d
python -m lab.cli wait --timeout 300
python -m lab.cli bootstrap
python -m lab.cli ingest specs/orders_raw.json

python -m lab.cli generate --count 1000 --late-every 5 --duplicate-every 10 \
  --bad-records 3 --schema-v2 --output data/first.jsonl
python -m lab.cli send data/first.jsonl
python -m lab.cli verify data/first.jsonl.manifest.json --timeout 300
python -m lab.cli query queries/revenue.sql
```

Druid console: http://localhost:8888. The basic smoke test performs the same steps: `make smoke` (the filename `data/smoke.jsonl` must be unused). The generator refuses to overwrite an existing archive; choose a new filename for the next run.

Expected reconciliation: **1100 valid records, 1000 unique event IDs**, and the monetary total from the manifest. The three malformed records are excluded from these 1100 records. Late events are accepted because the main supervisor does not configure late-event rejection. Check parse counters separately: the manifest alone does not prove what happened to malformed records.

`verify` polls Druid until the count, exact distinct count, and sum match. It exits with a nonzero status on a mismatch or timeout. A successful `send` confirms Kafka acknowledgments, not availability in Druid.

## Suggested learning path

| Step | Practice | Outcome |
|---|---|---|
| 1 | Architecture and happy path | Explain roles, offsets, segments, and real-time visibility |
| 2 | Late events and duplicates | Understand the boundaries of exactly-once guarantees and event time |
| 3 | Malformed events and schema evolution | Find parse errors and update a supervisor |
| 4 | Rollup and cardinality | Compare rows, SUM(events), storage size, and latency |
| 5 | Worker/Kafka failure | Recover ingestion and reconcile counts and sums |
| 6 | Batch rebuild | Produce an independent canonical datasource |
| 7 | Handoff and compaction | Verify Historical serving and segments |
| 8 | Observability and load | Collect lag, query latency, alerts, and observations |

Start with steps 1–3. Continue once you can trace an event from the producer to a query.

## Monitoring

In a separate terminal with `.venv` activated:

```bash
python -m lab.probe --host 0.0.0.0
```

In another terminal:

```bash
docker compose --profile monitoring up -d prometheus
```

Prometheus: http://localhost:9090; exporter: http://localhost:9108/metrics. Binding to `0.0.0.0` allows Prometheus to reach the probe from its container; run the probe on a trusted local network. The container ports themselves are published only on loopback.

The probe measures SQL availability, SELECT 1 latency, and the age of the latest visible producer timestamp. It is **neither a native Druid exporter nor a measure of Kafka offset lag**. When the producer stops, the age of the latest event naturally increases. Inspect actual ingestion lag in the supervisor status or console; it cannot be inferred from the consumer-group lag of a conventional Kafka consumer group.

## Operations

```bash
python -m lab.cli --help
python -m lab.cli status
python -m lab.cli query queries/tasks.sql
python -m lab.cli query queries/segments.sql
docker compose logs --tail 100 middlemanager coordinator
docker compose down
```

`down` preserves volumes. `docker compose down -v` deletes all environment data; use it only for a deliberate full reset. Files in the host's `data/` directory are separate and remain intact.

## Validation status

Docker is unavailable in the preparation environment. Unit tests and an offline generator smoke test were run, and YAML/JSON was checked. Container startup, Kafka → Druid ingestion, failover, and performance **have not been tested here**. The first local acceptance check is the command sequence under “First run.” The runbooks cover potential issues and diagnosis. The project does not yet report measured latency or throughput results.
