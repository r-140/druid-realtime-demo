# Architecture and engineering decisions

## Data path and control plane

The producer writes events to the Kafka topic `orders` (3 partitions). The Kafka supervisor in Overlord assigns partitions to indexing tasks. Tasks run as separate Java processes under MiddleManager, read Kafka offsets, build indexes, and serve real-time queries. During publication, segments are written to deep storage and metadata to Postgres; Historical loads the segments. Broker routes queries to the relevant Historical services and real-time tasks and combines the results. Router is the UI/API entry point.

In this environment, Coordinator and Overlord share the `coordinator` container. ZooKeeper supports Druid coordination and discovery. Kafka itself uses KRaft, not this ZooKeeper instance. Postgres stores Druid metadata, not sales events. Local deep storage is the shared `/opt/shared` volume; it is not distributed, fault-tolerant storage.

If a Historical service fails, its segments can be reloaded from deep storage. If a streaming worker fails, recovery depends on offsets, published segments, and data still being available within Kafka retention. Losing the metadata store or deep storage is a separate disaster-recovery scenario, not equivalent to restarting a worker.

## Event contract

| Field | Meaning |
|---|---|
| event_id | Business event ID; not an upsert key in Druid |
| order_id | Kafka partitioning key; the generator produces one sale or refund per order |
| run_id | An isolated run identifier, so reconciliation is independent of previous runs |
| event_time | Business event time; becomes Druid `__time` |
| ingested_at | Time the generator created the record, **not** Druid ingestion completion |
| amount_cents | Signed integer: positive for a sale, negative for a refund |
| currency | A separate dimension; totals across currencies require FX conversion |
| schema_version, campaign | Demonstration of additive schema evolution |

The generator models a ledger of facts. It does not implement a complete order lifecycle, payment authorization, partial refunds, or CDC. `net_amount_cents` is the sum of ledger events, not an accounting-reconciled revenue figure.

## Guarantees

Druid Kafka ingestion uses partition offsets to provide exactly-once ingestion within its publication and recovery protocol. If a producer writes the same business event at two different Kafka offsets, Druid does not deduplicate it by event_id. Producer idempotence protects against some retry-related duplicates within the producer protocol, but rerunning `send` is not a business-idempotent operation.

Druid is not designed for per-row point updates or UPSERTs. This project demonstrates correcting analytical data through a batch rebuild into a **new datasource**. Production use requires an explicit contract for the canonical source, event versions, deduplication, and cutover.

Rollup combines matching dimensions within a time bucket by aggregating metrics. It is aggregation, not deduplication: a repeated sale increases both the count and the sum. Raw retains event_id and ingested_at; the minute datasource excludes them to reduce cardinality. `run_id` is retained solely for lab reconciliation; a large number of run_id values also reduces rollup effectiveness.

Streaming rollup is best effort; rows belonging to the same logical bucket may appear in multiple segments. Queries must therefore use `SUM(events)` rather than count physical rows. Monetary totals here use exact integers; large production totals require LONG overflow checks. Study approximate distinct counts and quantiles separately, measuring their error.

## Time, lateness, and watermarks

`event_time` determines event-time buckets. `ingested_at` can estimate the age of visible producer events, but separating generation→Kafka delay from Kafka→Druid delay requires additional timestamps. MAX(event_time) may give a misleading view of freshness when events have future timestamps.

The main supervisor has no lateMessageRejectionPeriod: old events may reopen old time chunks and create small segments. The separate `orders_late_policy` supervisor sets PT1H. The cutoff is tied to the **indexing task's creation time**, rather than the maximum event time as with a Spark watermark. It is not a Spark watermark window or a universal boundary for window completeness. Very late data, retention, compaction, and batch replacement need separate coordination.

## Differences from production

There is one Kafka broker, one instance of each role, replicas=1, local volumes, and plaintext communication. The environment does not include HA/S3, TLS/auth/RBAC, a schema registry or DLQ, a CDC connector, autoscaling, Kubernetes manifests, GitOps, backup restoration, or a native metrics pipeline. Ports bind to localhost and passwords are for local learning only. Do not expose this environment to the internet.

Practical next steps after the local labs:

1. A native metrics emitter, Grafana, and alerts with a validated noise budget; collect task metrics separately from service metrics.
2. Object storage, metadata backups, and a tested restore procedure.
3. Kind/k3d, resource requests and limits, probes, persistent volumes, graceful termination, and a disruption exercise.
4. A second worker and replicas=2; three Kafka brokers; failure-domain analysis.
5. A canonical upstream pipeline with durable deduplication and a DLQ. Do not claim “exactly-once end-to-end” without checking every boundary.
6. A migration lab covering shadow queries, result comparison, schema compatibility, rollback, and cutover.

These items are a roadmap, not implemented v1 features.
