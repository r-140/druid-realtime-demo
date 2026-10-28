# Лабораторные

Все команды из корня проекта, `.venv` активирована. Исходная точка — успешный First Run из README. Сохраняй команды, manifest, supervisor status до/после и query outputs. Не выполняй несколько fault experiments одновременно.

## 1. Happy path и handoff

1. Найди supervisor `orders_raw` и running Kafka task в UI.
2. Выполни `python -m lab.cli query queries/segments.sql`.
3. Проверь count/sum командой `verify` из README.
4. Подожди публикацию: taskDuration=10 минут. Через UI посмотри, что старая задача SUCCESS, segments доступны на Historical, новый task читает следующие offsets.
5. Повтори verify. Результат должен остаться тем же.

Успех: одинаковый результат до и после handoff. Объясни, кто обслуживает запрос до загрузки Historical и где находятся сегменты/metadata.

## 2. Late events, дубли и replay

В `data/first.jsonl` 200 исходных событий старше на два часа; из-за дублирования соответствующих событий поздних записей окажется 300. Проверь в Query view по run_id из manifest:

```sql
SELECT TIME_FLOOR(__time, 'PT1H') AS hour, COUNT(*) AS n
FROM orders_raw WHERE run_id = 'RUN_ID_FROM_MANIFEST'
GROUP BY 1 ORDER BY 1;
```

Сумма n = 1100. `queries/duplicates.sql` использует последний день; для архивных событий расширь интервал.

Теперь повторно отправь `data/first.jsonl`. Для этого run_id ожидаются **2200** строк, **1000** unique event_id и удвоенная signed сумма. Исходный verify должен завершиться ошибкой — manifest описывает одну отправку. Это намеренный negative test. Idempotent Kafka producer не устраняет два явно выполненных send.

Эксперимент с late rejection (выполнять отдельно от rollup, чтобы не исчерпать task slots):

```bash
python -m lab.cli ingest specs/orders_late_policy.json
python -m lab.cli status --supervisor orders_late_policy
```

Новый supervisor читает topic с начала. В UI/SQL сравни с raw. Для только первой отправки ожидается 800 принятых записей, если все свежие timestamps ещё внутри границы task creation minus 1 hour. После replay — 1600. Не принимай эти цифры на веру при старом архиве: порог считается от создания задачи. Проверь thrownAway counters. После опыта suspend этот supervisor через UI и дождись завершения задач.

Успех: объяснить разницу offset-level recovery, business duplicates и late rejection; продемонстрировать три разных результата.

## 3. Ошибки парсинга и изменение схемы

Три malformed JSON из первого прогона должны отразиться в task parse counters/logs. Найди task в UI, посмотри Reports/Logs, отличай unparseable от thrownAway и processedWithError. Порог maxParseExceptions=100 — учебная настройка. Это не DLQ: Kafka остаётся источником replay только пока действует retention. maxSavedParseExceptions=10 сохраняет ограниченную диагностическую выборку.

`campaign` заранее объявлена nullable dimension; v1 её не отправляет, v2 заполняет. Сгенерируй v1 и v2 отдельными файлами/run_id, отправь и сверь каждый. Для v1 campaign будет отсутствующей/null, для v2 — заполненной.

Чтобы потренировать именно обновление supervisor, добавь dimension `device_type` в копию raw spec и поле в новый архив:

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

Дождись новой reading task со схемой device_type до send. Обновление supervisor сохраняет Kafka offset continuity; старые segments не получают новое поле задним числом. Проверь новый dimension в SQL. Manifest остаётся применим: count, event_id и amount не менялись. Смена типа существующего поля — другой, потенциально несовместимый сценарий: его не стоит делать поверх baseline.

## 4. Rollup, cardinality и latency

```bash
python -m lab.cli ingest specs/orders_minute.json
python -m lab.cli verify data/first.jsonl.manifest.json --datasource orders_minute --rollup
python -m lab.cli query queries/rollup.sql
python -m lab.benchmark queries/revenue.sql --iterations 30
python -m lab.benchmark queries/rollup.sql --iterations 30
```

Этот verify предполагает ровно один send исходного файла. Если уже был replay, выбери новый run_id и новый архив. Оба supervisors читают один topic независимо: это не Kafka consumer group load balancing между datasource.

Сравни физические rows в raw и minute после handoff и SUM(events) по run_id. Число физических строк rollup не обязано быть одним на logical bucket до compaction. При малом объёме скорости могут быть близки. Для измерения используй 100k/1m событий и сопоставимые time filters, dimensions и results; запиши resources, data size, segment count и cache conditions. Benchmark измеряет последовательные запросы, не concurrent throughput.

## 5. Worker failure и Kafka lag

Создай новый файл, чтобы отделить fault test от предыдущих replay:

```bash
python -m lab.cli generate --count 10000 --output data/worker-failure.jsonl
python -m lab.cli send data/worker-failure.jsonl --rate 200
```

Пока send выполняется, из второго терминала:

```bash
docker compose kill -s SIGKILL middlemanager
```

Подожди 30–60 секунд, зафиксируй supervisor status, logs и query behaviour. Затем:

```bash
docker compose up -d middlemanager
python -m lab.cli verify data/worker-failure.jsonl.manifest.json --timeout 600
```

Успех: count/distinct/sum совпали без повторного send. Зафиксируй время до healthy reading tasks, catch-up и handoff. При replicas=1 свежие realtime данные во время отказа могут временно быть недоступны.

Для Kafka outage отдельно: останови Kafka (`docker compose stop kafka`), наблюдай ошибки/lag, восстанови `docker compose start kafka`. Не запускай долгий send поверх outage с ожиданием бесконечного retry: delivery timeout=60s, после ошибки сначала выясни, какие записи подтверждены. Автоматический replay всего файла создаст business duplicates.

## 6. Canonical batch rebuild

Сначала suspend все supervisors через UI, дождись task completion. Это освобождает slots; source Kafka и старые datasource сохраняются. Используй новый datasource suffix:

```bash
python -m lab.cli backfill data/first.jsonl --datasource orders_rebuilt_v1
```

Команда удаляет только точные event_id duplicates из локального архива, отбрасывает malformed строки, отказывается молча разрешать конфликт разных payload одного ID. Создаёт canonical JSONL и native batch task, который читает общий bind mount. После task SUCCESS дождись segment availability и выполни в UI:

```sql
SELECT COUNT(*) AS n, COUNT(DISTINCT event_id) AS unique_n,
       SUM(amount_cents) AS amount
FROM orders_rebuilt_v1;
```

Для exact distinct отключи approximate count distinct в query context. Ожидаются 1000 строк и unique_amount_cents из manifest, а не исходная сумма с дублями. Этот источник содержит один конкретный run, а не всю историю Kafka. Rebuild демонстрирует подход, но не реализует полный cutover/rollback аналитического API. Возобнови supervisors после опыта.

## 7. Compaction и границы интервалов

1. Suspend supervisors; дождись завершения/publish и loaded Historical segments.
2. Сохрани count/sum и `queries/segments.sql` до изменений.
3. Выбери **один UTC hour interval** из segments UI, содержащий опубликованные данные.
4. Выполни, подставив свой интервал:

```bash
python -m lab.cli compact --datasource orders_raw \
  --interval '2026-10-28T08:00:00Z/2026-10-28T09:00:00Z'
```

Даты — пример; не запускай его вместо своего populated interval. После SUCCESS и availability повтори count/sum и segments query. Данные должны сохраниться; segment count/size могут измениться. На крошечном наборе compaction не обязана давать пользу. Raw rollup остаётся выключенным. Для minute проверь SUM(events), а не только физические строки. Возобнови supervisors.

## 8. Наблюдаемость и итоговое интервью

Включи probe и Prometheus по README. Выполни worker failure ещё раз и сопоставь:

- SQL up и query latency: это serving availability.
- Supervisor health, latest/current offsets, partition lag: ingestion progress.
- Parse counters: data quality.
- Worker capacity и publishing tasks: scheduling pressure.
- Pending/unavailable segments: handoff/serving progress.
- Producer timestamp age: полезен только при регулярно поступающем трафике.

Запиши report: версия, ресурсы, объём, сценарий, expected vs actual, время recovery, screenshots/outputs и вывод. Вопросы для себя: почему SQL может работать при сломанном ingestion; чем Kafka retention отличается от Druid retention; почему reset offsets опасен; как доказать отсутствие потерь; как rollback schema change; какие компоненты требуют HA.
