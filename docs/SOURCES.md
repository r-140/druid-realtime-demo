# Sources and baseline version

Checked on October 28, 2026. Druid 35.0.0 is a pinned learning baseline, not the latest release. The `latest` documentation may describe newer features; the project does not use recent-only supervisor features such as bounded stream configuration. Basic Docker conventions were checked against the release-tagged 35.0.0 files.

- Druid Docker quickstart: https://druid.apache.org/docs/latest/tutorials/docker/
- Release-tagged Compose example: https://github.com/apache/druid/blob/35.0.0/distribution/docker/docker-compose.yml
- Release-tagged environment conventions: https://github.com/apache/druid/blob/35.0.0/distribution/docker/environment
- Kafka ingestion and offsets: https://druid.apache.org/docs/latest/ingestion/kafka-ingestion/
- Supervisors, schema changes, rejection periods, and worker capacity: https://druid.apache.org/docs/latest/ingestion/supervisor/
- Input sources for native batch ingestion: https://druid.apache.org/docs/latest/ingestion/input-sources/
- Task reports and parse counters: https://druid.apache.org/docs/latest/ingestion/tasks/
- Druid metrics: https://druid.apache.org/docs/latest/operations/metrics/
- Native Prometheus emitter (roadmap; v1 uses a custom probe): https://druid.apache.org/docs/latest/development/extensions-contrib/prometheus.html/
- Official Apache Kafka JVM Docker image: https://kafka.apache.org/40/getting-started/docker/

The README and documentation are learning materials, not claims about a particular company's workload or the internal architecture of the TEK role.

## Flink extension — October 29, 2026

The Flink/PyFlink version is pinned to 1.20.1 and the shaded Kafka connector to 3.3.0-1.20. The 1.20 documentation may show a later patch release. API signatures were also inspected in Apache Flink's release-1.20.1 source.

- Kafka source/sink, transactions, and checkpoint visibility: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/
- Python Kafka connector API: https://nightlies.apache.org/flink/flink-docs-release-1.20/api/python/_modules/pyflink/datastream/connectors/kafka.html
- Python installation and supported Python versions: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/installation/
- Docker deployment: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/
- CLI stop/savepoint/restore: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/cli/
- Savepoint semantics and operator IDs: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/
- Release-tagged Python state/TTL API: https://github.com/apache/flink/blob/release-1.20.1/flink-python/pyflink/datastream/state.py
