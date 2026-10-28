# Архитектура и инженерные решения

## Data path и control plane

Producer пишет события в Kafka `orders` (3 partitions). Kafka supervisor в Overlord распределяет partitions между indexing tasks. Tasks выполняются отдельными Java-процессами у MiddleManager, читают Kafka offsets, индексируют и отвечают на realtime queries. При публикации segments попадают в deep storage, metadata — в Postgres; Historical загружает segments. Broker маршрутизирует запросы к нужным Historical/realtime tasks и объединяет результаты. Router — UI/API вход.

В стенде Coordinator и Overlord совмещены в контейнере `coordinator`. ZooKeeper участвует в координации и discovery Druid. Сам Kafka использует KRaft, а не этот ZooKeeper. Postgres хранит Druid metadata, не события продаж. Local deep storage — общий volume `/opt/shared`; это не распределённое отказоустойчивое хранилище.

При отказе Historical сегменты можно снова загрузить из deep storage. При отказе streaming worker восстановление зависит от offsets, published segments и доступности Kafka retention. Потеря metadata store или deep storage — отдельные сценарии disaster recovery, не эквивалент restart worker.

## Контракт событий

| Поле | Семантика |
|---|---|
| event_id | ID бизнес-события. Не ключ upsert в Druid |
| order_id | Kafka key для partitioning; в генераторе одна продажа либо возврат на order |
| run_id | Метка изолированного прогона, чтобы сверка не зависела от старых прогонов |
| event_time | Время бизнес-события, становится Druid `__time` |
| ingested_at | Время создания записи генератором, **не** Druid ingestion completion |
| amount_cents | Signed integer: sale положителен, refund отрицателен |
| currency | Отдельная размерность; суммы разных валют нельзя складывать без FX |
| schema_version, campaign | Демонстрация additive schema evolution |

Генератор моделирует ledger фактов. Он не реализует полноценный lifecycle заказа, payment authorization, частичные возвраты и CDC. `net_amount_cents` — сумма событий ledger, а не бухгалтерски выверенная выручка.

## Гарантии

Druid Kafka ingestion использует partition offsets для exactly-once ingestion в своём протоколе публикации/восстановления. Если producer пишет то же бизнес-событие в два разных Kafka offsets, Druid не устраняет такой дубль по event_id. Producer idempotence защищает от части дубликатов из-за retries в рамках producer protocol, но не превращает повторный запуск `send` в бизнес-idempotent операцию.

Druid не предназначен для point updates/UPSERT каждой строки. Исправление аналитических данных здесь показано через batch rebuild в **новый datasource**. В production потребуется явный контракт canonical source, версий событий, дедупликации и cutover.

Rollup объединяет одинаковые dimensions и time bucket, суммируя metrics. Это агрегация, а не deduplication: повтор sale увеличит count и сумму. Raw сохраняет event_id и ingested_at; minute datasource исключает их ради более низкой cardinality. `run_id` оставлен только для лабораторной сверки; большое число run_id тоже ухудшает rollup.

Streaming rollup — best effort; строки одного logical bucket могут находиться в нескольких segments. Поэтому запрос должен делать `SUM(events)`, а не считать физические строки. Финансовые суммы здесь exact integers; большие production итоги требуют контроля переполнения LONG. Approximate distinct/quantiles нужно изучать отдельно с измерением ошибки.

## Время, lateness и watermark

`event_time` определяет event-time buckets. `ingested_at` пригоден для оценки возраста видимых producer events, но задержка генерация→Kafka неотделима от Kafka→Druid без дополнительных timestamps. MAX(event_time) может давать ложную картину свежести при future timestamps.

У основного supervisor нет lateMessageRejectionPeriod: старые события могут открыть старые time chunks и создать маленькие segments. Отдельный `orders_late_policy` задаёт PT1H. Порог привязан к **времени создания indexing task**, а не к максимальному event time, как watermark в Spark. Это не Spark watermark window и не универсальная граница завершённости окна. Very late data, retention, compaction и batch replacement надо согласовывать отдельно.

## Что отличается от production

Один broker Kafka, один экземпляр каждой роли, replicas=1, локальные volumes и plaintext. Нет HA/S3, TLS/auth/RBAC, schema registry/DLQ, CDC connector, autoscaling, Kubernetes manifests, GitOps, backup restore, native metrics pipeline. Ports привязаны к localhost; пароли учебные. Не публикуй стенд в интернет.

Практические следующие этапы после локальных labs:

1. Native metrics emitter + Grafana + уведомления с проверенным noise budget; отдельно task metrics и service metrics.
2. Object storage, резервное копирование metadata и проверяемый restore.
3. Kind/k3d, resource requests/limits, probes, persistent volumes, graceful termination и disruption exercise.
4. Второй worker и replicas=2; Kafka 3 brokers; failure-domain analysis.
5. Canonical upstream pipeline с durable dedup и DLQ. Не писать «exactly-once end-to-end» без проверки всех границ.
6. Миграционная лаборатория: shadow queries, compare results, schema compatibility, rollback, cutover.

Эти пункты — roadmap, не реализованные возможности v1.
