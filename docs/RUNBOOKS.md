# Runbooks: local real-time platform

General sequence: record scope and timestamps → check serving separately from ingestion → find the limiting component → apply a minimal fix → verify counts, sums, and handoff. Do not start by resetting offsets.

## SQL is unavailable / the console shows errors

1. Run `docker compose ps` and `docker compose logs --tail 100 router broker coordinator`.
2. Check host memory, disk, OOM events, and restart counts using `docker stats --no-stream` and `docker inspect` for the relevant container.
3. If Router is reachable but SQL is not, check Broker and discovery. If SQL SELECT 1 succeeds but the datasource is missing, check the supervisor, tasks, and segments; SELECT 1 alone does not prove ingestion is healthy.
4. If only expensive queries time out, narrow the time range and check cardinality, the query plan in the console, and processing memory. Do not begin by arbitrarily increasing the timeout.
5. After the fix, run `python -m lab.cli wait`, then verify the relevant manifest.

## Supervisor is unhealthy / lag is growing

1. Run `python -m lab.cli status`; save healthy, state, recentErrors, partitions, latest/current offsets, and lag. Available fields depend on the version and task state.
2. Run `python -m lab.cli query queries/tasks.sql`; inspect task reports/logs through the console.
3. Check Kafka reachability: the host producer uses `localhost:9092`; Druid inside Docker uses `kafka:29092`. Do not use localhost in the supervisor configuration.
4. Check that the topic exists (`python -m lab.cli bootstrap` creates it only if absent). With partitions=3 and taskCount=1, one reading task reads three partitions. Raising taskCount above the number of partitions adds no reading concurrency.
5. Check slots: replicas × taskCount reading tasks, plus publishing, batch, and compaction tasks. Do not increase concurrency without sufficient RAM and task slots.
6. After recovery, confirm lag is decreasing, then verify counts, distinct counts, sums, and handoff. Zero lag without reconciliation does not prove payload correctness.

## Parse exceptions / missing rows

1. Compare producer-acknowledged records with manifest valid_records and bad_records. After invalid records are dropped, Druid COUNT(*) should not equal the total number of Kafka records.
2. Inspect reports: unparseable, processedWithError, and thrownAway. Check timestamp parsing, numeric field types, and late rejection.
3. Inspect payload samples from the saved archive; do not write production PII to diagnostic logs.
4. Fix the producer/schema. For data already lost during ingestion, prepare a canonical rebuild and reconciliation, accounting for Kafka retention and offsets.
5. Do not treat maxSavedParseExceptions as a full DLQ. This project does not include automatic DLQ routing.

## Totals doubled after recovery

1. Compare COUNT(*) with COUNT(DISTINCT event_id), using an exact-distinct query context.
2. Find repeated IDs with `queries/duplicates.sql`, using the appropriate time range.
3. Check the history of `send` calls, new producer sessions, and offset resets. Two Kafka offsets with the same event_id represent two events to Druid.
4. Compaction does not fix this: rollup adds duplicates rather than subtracting them.
5. In the lab, rebuild a separate orders_rebuilt_<version> datasource from the canonical archive and verify unique counts and sums. In production, define event-version and conflict-resolution policies before cutover.

## Tasks are stuck publishing / segments are unavailable

1. Inspect task status/reports and Coordinator/Historical logs. Task completion and query availability are different states.
2. Check the `/opt/shared` volume, free disk space, permissions, and metadata Postgres availability. A single local shared volume is an intentional limitation of this environment.
3. Check availability in the segments UI and with `queries/segments.sql`. Distinguish active/published segments from segments that are actually loaded/available.
4. Compare queries before and after handoff. Restarting without checking metadata and deep storage is not disaster recovery.

## OOM / many small segments

1. Check host/container memory, the number of peon JVMs, heap, and direct buffers; heap is only part of Druid's memory footprint.
2. Suspend optional supervisors and wait for tasks to finish. Start with the raw-only configuration; do not run rebuilds alongside two supervisors' reading/publishing tasks on a small machine.
3. Small segments can result from the short taskDuration=10m and late events. In production, choose granularity, task duration, target segment size, and compaction settings based on measurements.
4. Compact a published, closed interval, then verify counts, sums, and new segments. Savings are not guaranteed on tiny datasets.

## Full reset

`docker compose down` preserves data. Removing volumes loses metadata, segments, and Kafka history. For a deliberate full reset of this disposable environment: stop the producer/probe, run `docker compose down -v`, choose a new archive output path, and repeat “First run.” Removing volumes does not delete the host's `data/` directory.

## Flink path issues

See the [Flink troubleshooting section](FLINK.md#troubleshooting-and-operational-signals). Check job state, completed checkpoints, committed Kafka output, and Druid ingestion separately. A fresh Flink submission without state restoration can replay old events into existing output topics. The base Druid Prometheus probe does not cover Flink checkpoint health or custom counters.
