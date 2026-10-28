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
