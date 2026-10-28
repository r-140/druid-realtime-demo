# Druid Real-time Platform Lab

Практическая лаборатория для знакомства с Apache Druid и эксплуатацией real-time OLAP платформы. Предметная область — события продаж и возвратов сети магазинов. Python + Apache Kafka + Apache Druid + PostgreSQL + ZooKeeper; опционально Prometheus.

Цель — уметь объяснить путь события до SQL, проверить гарантии доставки и восстановить ingestion после сбоя. Проект содержит локальный стенд и воспроизводимые упражнения. Он не является production deployment.

## Что уже реализовано

- Docker Compose с отдельными ролями Druid и постоянными volumes.
- Kafka topic с тремя partitions и idempotent producer.
- Python CLI: генерация, отправка, supervisor API, SQL, reconciliation, batch rebuild, compaction.
- Генератор late events, бизнес-дубликатов, malformed JSON и schema v2.
- Raw datasource и опциональный minute rollup, отдельная late-rejection policy.
- Манифесты ожидаемых count, distinct event count и денежных сумм.
- SQL-примеры, latency benchmark, application-level metrics probe и Prometheus scrape config.
- Пошаговые [лабораторные](docs/LABS.md), [runbooks](docs/RUNBOOKS.md), [архитектура и ограничения](docs/ARCHITECTURE.md).

## Предпосылки

Linux/macOS/WSL2, Docker Engine или Docker Desktop с Compose v2, Python 3.11+, интернет для загрузки образов и зависимостей. Команды ниже выполняются из корня проекта в bash. `make` необязателен.

Ориентир для полной конфигурации: выделить Docker 12–16 GiB RAM, иметь около 20 GiB свободного диска. Это оценка, а не измеренное минимальное требование. На малой машине начинать с одного `orders_raw` supervisor. У MiddleManager четыре task slots, чтобы reading и publishing задачи двух supervisors могли некоторое время сосуществовать. Rebuild/compaction выполнять отдельно от активной streaming нагрузки.

Версии намеренно закреплены: Druid 35.0.0, Kafka 4.0.2, Postgres 16.8, ZooKeeper 3.9.3, Prometheus 3.2.1. Это воспроизводимый учебный baseline, не заявление о последних релизах. Config сверялся с официальным Druid Docker quickstart и документацией ingestion; ссылки в docs/SOURCES.md.

## Первый запуск

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

Druid console: http://localhost:8888. Базовый smoke делает те же шаги: `make smoke` (имя `data/smoke.jsonl` должно быть свободно). Генератор отказывается перезаписывать существующий архив: для следующего прогона выбери новое имя.

Ожидаемая сверка: **1100 valid records, 1000 unique event IDs**, сумма из manifest. Три malformed записи в эти 1100 не входят. Поздние события принимаются, потому что основной supervisor не задаёт late rejection. Parse counters проверяются отдельно: manifest сам по себе не доказывает, где оказались плохие записи.

`verify` опрашивает Druid до совпадения count, exact distinct и суммы; завершает процесс с ненулевым кодом при расхождении/таймауте. Успешный `send` подтверждает Kafka acknowledgments, а не доступность в Druid.

## Как идти по проекту

| Шаг | Практика | Результат |
|---|---|---|
| 1 | Архитектура и happy path | Объяснить роли, offsets, сегменты, realtime visibility |
| 2 | Late events и duplicates | Увидеть границы exactly-once и event time |
| 3 | Malformed events и schema evolution | Найти parse errors, обновить supervisor |
| 4 | Rollup и cardinality | Сравнить строки, SUM(events), объём и latency |
| 5 | Worker/Kafka failure | Восстановить ingestion и проверить count/sum |
| 6 | Batch rebuild | Получить независимый canonical datasource |
| 7 | Handoff и compaction | Проверить Historical serving и сегменты |
| 8 | Observability и нагрузка | Собрать lag, query latency, alerts и заметки |

Начни с шагов 1–3. Остальные разумнее делать после того, как можешь проследить событие от producer до query.

## Мониторинг

В отдельном терминале с активированной `.venv`:

```bash
python -m lab.probe --host 0.0.0.0
```

В другом:

```bash
docker compose --profile monitoring up -d prometheus
```

Prometheus: http://localhost:9090; exporter: http://localhost:9108/metrics. Адрес `0.0.0.0` нужен для доступа Prometheus из контейнера; запускай probe в доверенной локальной сети. Сами container ports опубликованы только на loopback.

Probe измеряет SQL availability, SELECT 1 latency и возраст последнего видимого producer timestamp. Это **не native Druid exporter и не Kafka offset lag**. Когда producer остановлен, возраст последнего события закономерно растёт. Настоящий ingestion lag смотри в supervisor status/console; его нельзя выводить из consumer-group lag обычной Kafka consumer group.

## Управление

```bash
python -m lab.cli --help
python -m lab.cli status
python -m lab.cli query queries/tasks.sql
python -m lab.cli query queries/segments.sql
docker compose logs --tail 100 middlemanager coordinator
docker compose down
```

`down` сохраняет volumes. `docker compose down -v` удаляет все данные стенда; используй только для осознанного полного сброса. Файлы `data/` находятся на хосте и остаются отдельно.

## Статус проверки

В среде подготовки нет Docker. Выполнены unit tests и offline smoke генератора, проверен YAML/JSON. Старт контейнеров, Kafka → Druid ingestion, failover и performance **здесь не проверены**. Первый локальный acceptance check — команды раздела «Первый запуск». Возможные проблемы и диагностика описаны в runbooks. Измеренных latency/throughput результатов проект пока не заявляет.
