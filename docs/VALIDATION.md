# Проверка v1 — 28 октября 2026

Выполнено в Python 3.12.14:

- `python -m unittest discover -s tests -v`: 7 tests PASS.
- `compileall` для lab/: PASS.
- YAML parse compose/prometheus и JSON parse supervisor specs: PASS.
- Проверка закреплённых image tags, loopback port bindings и существования bind mount sources: PASS.
- Offline генерация с fixed UTC time и run_id: 1000 исходных событий, duplicate_every=10, late_every=5, schema v2, 3 malformed записи.
- Независимая сверка файла: 1100 valid records, 1000 unique IDs, 100 duplicate records, 300 late records с учётом дубликатов, 3 malformed lines; сумма valid amount_cents совпала с manifest.

Не выполнено: Docker Compose schema validation самим Docker, image pulls, старт services, Kafka protocol connection, Druid ingestion/SQL, handoff, failure recovery, native batch/compaction tasks, Prometheus scraping и benchmarks на работающем кластере. В среде подготовки нет docker binary.

Unit tests проверяют Python/event contract и конфигурационные инварианты. Они не подменяют integration tests Druid. Локальный acceptance check описан в README; лабораторные ожидаемые результаты — критерии приёмки, а не уже измеренные результаты.
