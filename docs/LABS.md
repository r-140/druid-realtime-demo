# Labs

Run all commands from the project root with `.venv` activated. Start after successfully completing “First run” in the README. Save commands, manifests, supervisor status before and after each experiment, and query outputs. Run one failure experiment at a time.

## 1. Happy path and handoff

1. Find the `orders_raw` supervisor and its running Kafka task in the UI.
2. Run `python -m lab.cli query queries/segments.sql`.
3. Check the count and sum using the README's `verify` command.
4. Wait for publication: taskDuration=10 minutes. In the UI, confirm that the old task reaches SUCCESS, segments become available on Historical, and a new task reads the next offsets.
5. Repeat verify. The result should remain unchanged.

Success criterion: identical results before and after handoff. Explain what serves queries before Historical loads the data and where segments and metadata reside.

## 2. Late events, duplicates, and replay

In `data/first.jsonl`, 200 original events are two hours old; duplicating some of these events produces 300 late records in total. In the Query view, filter by the run_id from the manifest:

```sql
SELECT TIME_FLOOR(__time, 'PT1H') AS hour, COUNT(*) AS n
FROM orders_raw WHERE run_id = 'RUN_ID_FROM_MANIFEST'
GROUP BY 1 ORDER BY 1;
```

The sum of n is 1100. `queries/duplicates.sql` covers the past day; widen the interval for older archived events.

Now send `data/first.jsonl` again. For this run_id, expect **2200** rows, **1000** unique event_id values, and twice the signed sum. The original verify should fail because the manifest describes one send. This is an intentional negative test. An idempotent Kafka producer does not deduplicate two explicitly executed sends.

Late-rejection experiment (run separately from rollup to avoid exhausting task slots):

```bash
python -m lab.cli ingest specs/orders_late_policy.json
python -m lab.cli status --supervisor orders_late_policy
```

The new supervisor reads the topic from the beginning. Compare its results with raw using the UI/SQL. With only the first send, expect 800 accepted records, provided all fresh timestamps remain within the cutoff of task creation time minus one hour. After replay, expect 1600. Do not assume these counts for an old archive: the cutoff is based on task creation time. Check thrownAway counters. After the experiment, suspend this supervisor through the UI and wait for its tasks to complete.

Success criterion: explain offset-level recovery, business duplicates, and late rejection, and demonstrate their different outcomes.

## 3. Parse errors and schema evolution

The three malformed JSON records from the first run should appear in task parse counters/logs. Find the task in the UI, inspect Reports/Logs, and distinguish unparseable from thrownAway and processedWithError. The maxParseExceptions=100 threshold is a learning configuration. This is not a DLQ: Kafka remains a replay source only while retention preserves the data. maxSavedParseExceptions=10 keeps a limited diagnostic sample.

`campaign` is declared in advance as a nullable dimension; v1 does not send it, while v2 populates it. Generate v1 and v2 as separate files/run_id values, send them, and reconcile each run. For v1, campaign will be missing/null; for v2, it will be populated.

To practice an actual supervisor update, add the `device_type` dimension to a copy of the raw spec and add the field to a new archive:

```bash
python - <<'PY'
import json
from pathlib import Path
p = Path('specs/orders_raw.json')
s = json.loads(p.read_text())
s['spec']['dataSchema']['dimensionsSpec']['dimensions'].append({'name':'device_type','type':'string'})
Path('data/raw-device-spec.json').write_text(json.dumps(s, indent=2))
PY
python -m lab.cli ingest data/raw-device-spec.json
python -m lab.cli generate --count 100 --schema-v2 --output data/device.jsonl
python - <<'PY'
import json
from pathlib import Path
p = Path('data/device.jsonl')
events = [json.loads(line) for line in p.read_text().splitlines()]
for e in events:
    e['device_type'] = 'mobile'
p.write_text(''.join(json.dumps(e)+'\n' for e in events))
PY
python -m lab.cli send data/device.jsonl
python -m lab.cli verify data/device.jsonl.manifest.json
```

Wait for a new reading task with the device_type schema before sending the file. Updating the supervisor preserves Kafka offset continuity; old segments do not receive the new field retroactively. Check the new dimension in SQL. The manifest still applies because the count, event_id values, and amounts did not change. Changing the type of an existing field is a different, potentially incompatible scenario; avoid doing it over the baseline.

## 4. Rollup, cardinality, and latency

```bash
python -m lab.cli ingest specs/orders_minute.json
python -m lab.cli verify data/first.jsonl.manifest.json --datasource orders_minute --rollup
python -m lab.cli query queries/rollup.sql
python -m lab.benchmark queries/revenue.sql --iterations 30
python -m lab.benchmark queries/rollup.sql --iterations 30
```

This verify assumes exactly one send of the original file. If you have already replayed it, choose a new run_id and a new archive. Both supervisors read the same topic independently; this is not Kafka consumer-group load balancing between datasources.

Compare physical rows in raw and minute after handoff, and compare SUM(events) by run_id. Before compaction, rollup does not guarantee one physical row per logical bucket. Performance may be similar on a small dataset. For measurements, use 100k/1m events and comparable time filters, dimensions, and results; record resources, data size, segment count, and cache conditions. The benchmark measures sequential queries, not concurrent throughput.

## 5. Worker failure and Kafka lag

Create a new file to isolate the failure test from earlier replays:

```bash
python -m lab.cli generate --count 10000 --output data/worker-failure.jsonl
python -m lab.cli send data/worker-failure.jsonl --rate 200
```

While send is running, use a second terminal:

```bash
docker compose kill -s SIGKILL middlemanager
```

Wait 30–60 seconds and capture supervisor status, logs, and query behavior. Then:

```bash
docker compose up -d middlemanager
python -m lab.cli verify data/worker-failure.jsonl.manifest.json --timeout 600
```

Success criterion: count, distinct count, and sum match without resending the file. Record the time until reading tasks become healthy, catch-up completes, and handoff occurs. With replicas=1, fresh real-time data may temporarily be unavailable during the failure.

Test a Kafka outage separately: stop Kafka (`docker compose stop kafka`), observe errors/lag, and recover with `docker compose start kafka`. Do not run a long send through an outage expecting unlimited retries: delivery timeout=60s. After an error, first determine which records were acknowledged. Automatically replaying the entire file creates business duplicates.

## 6. Canonical batch rebuild

First suspend all supervisors through the UI and wait for task completion. This frees task slots while retaining the source Kafka data and existing datasources. Use a new datasource suffix:

```bash
python -m lab.cli backfill data/first.jsonl --datasource orders_rebuilt_v1
```

The command removes only exact event_id duplicates from the local archive, drops malformed lines, and refuses to silently resolve conflicting payloads for the same ID. It creates canonical JSONL and a native batch task that reads the shared bind mount. After task SUCCESS, wait for segment availability and run this in the UI:

```sql
SELECT COUNT(*) AS n, COUNT(DISTINCT event_id) AS unique_n,
       SUM(amount_cents) AS amount
FROM orders_rebuilt_v1;
```

For exact distinct counting, disable approximate count distinct in the query context. Expect 1000 rows and unique_amount_cents from the manifest, rather than the original sum containing duplicates. This source contains one specific run, not the entire Kafka history. The rebuild demonstrates the approach but does not implement a full analytical API cutover/rollback. Resume the supervisors after the experiment.

## 7. Compaction and interval boundaries

1. Suspend supervisors; wait for task completion/publication and for Historical to load the segments.
2. Save the count, sum, and `queries/segments.sql` results before making changes.
3. Choose **one UTC hour interval** containing published data from the segments UI.
4. Run the following command, substituting your interval:

```bash
python -m lab.cli compact --datasource orders_raw \
  --interval '2026-10-28T08:00:00Z/2026-10-28T09:00:00Z'
```

The dates are examples; substitute your own populated interval. After SUCCESS and segment availability, repeat the count, sum, and segments query. The data should be preserved; segment count and size may change. Compaction may provide no benefit on a tiny dataset. Raw rollup remains disabled. For minute, check SUM(events), not just physical rows. Resume the supervisors.

## 8. Observability and interview review

Enable the probe and Prometheus as described in the README. Repeat the worker failure and correlate:

- SQL up and query latency: serving availability.
- Supervisor health, latest/current offsets, and partition lag: ingestion progress.
- Parse counters: data quality.
- Worker capacity and publishing tasks: scheduling pressure.
- Pending/unavailable segments: handoff and serving progress.
- Producer timestamp age: useful only when traffic arrives regularly.

Write a report covering versions, resources, volume, scenario, expected versus actual results, recovery time, screenshots/outputs, and conclusions. Questions to answer: why can SQL work while ingestion is broken; how does Kafka retention differ from Druid retention; why is resetting offsets risky; how can you demonstrate the absence of data loss; how would you roll back a schema change; which components require HA?
