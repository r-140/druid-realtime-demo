# Источники и версия baseline

Сверено 28 октября 2026. Druid 35.0.0 выбран как закреплённый учебный baseline, а не latest. `latest` docs могут описывать возможности более новых релизов; в проект не включены recent-only supervisor features вроде bounded stream config. Базовые Docker conventions сверялись с release-tagged 35.0.0 файлами.

- Druid Docker quickstart: https://druid.apache.org/docs/latest/tutorials/docker/
- Release-tagged Compose example: https://github.com/apache/druid/blob/35.0.0/distribution/docker/docker-compose.yml
- Release-tagged environment conventions: https://github.com/apache/druid/blob/35.0.0/distribution/docker/environment
- Kafka ingestion и offsets: https://druid.apache.org/docs/latest/ingestion/kafka-ingestion/
- Supervisors, schema change, rejection periods и worker capacity: https://druid.apache.org/docs/latest/ingestion/supervisor/
- Input sources для native batch: https://druid.apache.org/docs/latest/ingestion/input-sources/
- Task reports и parse counters: https://druid.apache.org/docs/latest/ingestion/tasks/
- Druid metrics: https://druid.apache.org/docs/latest/operations/metrics/
- Native Prometheus emitter (roadmap; в v1 используется собственный probe): https://druid.apache.org/docs/latest/development/extensions-contrib/prometheus.html/
- Official Apache Kafka JVM Docker image: https://kafka.apache.org/40/getting-started/docker/

README и docs — учебное объяснение, не утверждение о нагрузке конкретной компании или внутренней архитектуре вакансии TEK.
