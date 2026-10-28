# Runbooks: локальная real-time платформа

Общий порядок: зафиксировать scope и timestamps → проверить serving отдельно от ingestion → найти ограничивающий компонент → применить минимальное исправление → проверить count/sum и handoff. Не reset offsets первым действием.

## SQL не отвечает / console показывает ошибки

1. `docker compose ps`; `docker compose logs --tail 100 router broker coordinator`.
2. Проверь host memory, disk, OOM/restart count: `docker stats --no-stream`, `docker inspect` конкретного контейнера.
3. Router доступен, но SQL нет: проверить Broker и discovery. SQL SELECT 1 успешен, но datasource нет: проверить supervisor/tasks/segments, это ещё не доказательство исправного ingestion.
4. Если query timeout только на тяжёлых запросах: ограничить time range, проверить cardinality, query plan в console и processing memory. Не начинать с произвольного увеличения timeout.
5. После исправления `python -m lab.cli wait`, затем verify для нужного manifest.

## Supervisor unhealthy / lag растёт

1. `python -m lab.cli status`; сохранить healthy, state, recentErrors, partitions, latest/current offsets и lag. Набор полей зависит от версии и состояния задачи.
2. `python -m lab.cli query queries/tasks.sql`; посмотреть task reports/logs через console.
3. Проверить Kafka reachability: host producer использует `localhost:9092`, Druid внутри Docker — `kafka:29092`. Нельзя подставлять localhost в supervisor.
4. Проверить topic existence (`python -m lab.cli bootstrap` создаёт только при отсутствии). Partitions=3, taskCount=1: одна reading task читает три partitions. Увеличение taskCount выше числа partitions не даёт дополнительной reading concurrency.
5. Проверить slots: replicas × taskCount reading tasks плюс задачи публикации, batch и compaction. Не повышать concurrency без RAM и slots.
6. После восстановления убедиться, что lag убывает, затем count/distinct/sum и handoff. Lag=0 без reconciliation не доказывает корректность payload.

## Parse exceptions / missing rows

1. Сопоставить producer acknowledged records, manifest valid_records и bad_records. Не ожидать число Kafka records в COUNT(*) Druid после отбрасывания невалидных строк.
2. Посмотреть reports: unparseable, processedWithError, thrownAway. Уточнить timestamp parsing, тип numeric field и late rejection.
3. Отобрать payload из сохранённого архива; не сохранять production PII в диагностических логах.
4. Исправить producer/schema. Для уже потерянных при ingestion данных подготовить canonical rebuild и сверку, учитывая Kafka retention и offsets.
5. Не считать maxSavedParseExceptions полноценным DLQ. Этот проект не содержит автоматического DLQ маршрута.

## Двойные суммы после recovery

1. Проверить COUNT(*) против COUNT(DISTINCT event_id) с exact distinct context.
2. Найти повторные ID (`queries/duplicates.sql`, корректный time range).
3. Проверить историю `send`, использование нового producer session и reset offsets. Два Kafka offsets с одинаковым event_id — два события для Druid.
4. Не устранять проблему compaction: rollup суммирует повторы, не вычитает их.
5. В лаборатории восстановить отдельный orders_rebuilt_<version> datasource из canonical archive, проверить unique count/sum. В production определить event version/conflict policy до cutover.

## Tasks зависли в publishing / segments недоступны

1. Посмотреть task status/report и Coordinator/Historical logs. Task completion и query availability — разные состояния.
2. Проверить `/opt/shared` volume, free disk, permissions и metadata Postgres availability. Один локальный общий volume — намеренное ограничение стенда.
3. Проверить availability в segments UI и `queries/segments.sql`. Не путать active/published и реально loaded/available.
4. После handoff проверить запросы до/после. Restart без проверки metadata/deep storage не является disaster recovery.

## OOM / большое число маленьких segments

1. Проверить host/container memory, число peon JVM, heap/direct buffers; heap — только часть памяти Druid.
2. Suspend необязательные supervisors, дождаться завершения tasks. Сначала raw-only конфигурация; не запускай rebuild одновременно с двумя reading/publishing supervisors на маленькой машине.
3. Маленькие segments здесь возможны из-за короткого taskDuration=10m и late events. В production подбирать granularity, task duration, target segment size и compaction по измерениям.
4. Compaction на опубликованном закрытом интервале, затем count/sum и проверка новых segments. На крошечных данных экономия не гарантирована.

## Полный сброс

`docker compose down` сохраняет данные. Удаление volumes — потеря metadata, segments и Kafka history. Если нужен полный disposable reset: остановить producer/probe, осознанно выполнить `docker compose down -v`, выбрать новый archive output path, затем First Run. Удаление volumes не удаляет host `data/`.
