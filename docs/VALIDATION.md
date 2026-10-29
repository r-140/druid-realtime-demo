# v1 validation — October 28, 2026

Completed using Python 3.12.14:

- `python -m unittest discover -s tests -v`: 7 tests PASS.
- `compileall` for lab/: PASS.
- YAML parsing for Compose/Prometheus and JSON parsing for supervisor specs: PASS.
- Checks for pinned image tags, loopback port bindings, and existing bind-mount sources: PASS.
- Offline generation with a fixed UTC timestamp and run_id: 1000 original events, duplicate_every=10, late_every=5, schema v2, and 3 malformed records.
- Independent file reconciliation: 1100 valid records, 1000 unique IDs, 100 duplicate records, 300 late records including duplicates, and 3 malformed lines; the sum of valid amount_cents values matched the manifest.

Not completed: Docker Compose schema validation by Docker itself, image pulls, service startup, Kafka protocol connectivity, Druid ingestion/SQL, handoff, failure recovery, native batch/compaction tasks, Prometheus scraping, or benchmarks against a running cluster. The preparation environment has no Docker binary.

Unit tests check the Python/event contract and configuration invariants. They do not replace Druid integration tests. The local acceptance check is documented in the README; expected lab outcomes are acceptance criteria, not previously measured results.

## Flink extension — October 29, 2026

- Full offline unit suite: 17 tests PASS (7 existing, 10 additional tests for validation, fingerprint/disposition, enrichment, audit envelopes, clean reconciliation, and detection of a transient match followed by duplicate output).
- The shared pure-Python processing logic was simulated on 1103 input records: 1000 clean, 100 duplicate audits, 3 validation errors. Of the clean events, 200 retain a two-hour event-time delay. Signed clean sums matched unique_amount_cents.
- Python syntax compilation, JSON/YAML parsing, local documentation links, loopback port bindings, and Flink profile configuration were checked.
- Flink API method signatures were inspected against release-1.20.1 source.
- The TTL test checks the disposition when state is missing; it does not exercise Flink's TTL clock or cleanup implementation.

Not executed: custom image build, PyFlink import/job graph on a Flink runtime, Kafka transactions, Flink checkpoints/TTL/savepoint recovery, TaskManager failure, and end-to-end clean Druid ingestion. Docker is unavailable. All such procedures in FLINK.md remain local integration acceptance checks.
