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
