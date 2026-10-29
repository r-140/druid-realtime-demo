"""Kafka orders -> validation -> keyed dedup/enrichment -> transactional Kafka sinks."""
import json
import os

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig

from stream_contract import decode_validate, ContractError, timestamp_ms, canonical, fingerprint, disposition, enrich, audit

DLQ = OutputTag('invalid-or-conflict-v1', Types.STRING())
DUPLICATES = OutputTag('duplicate-audit-v1', Types.STRING())


class Validate(ProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        group = runtime_context.get_metrics_group()
        self.accepted = group.counter('contract_valid')
        self.rejected = group.counter('contract_invalid')

    def process_element(self, value, ctx):
        try:
            event = decode_validate(value)
        except ContractError as exc:
            self.rejected.inc()
            yield DLQ, audit('validation: ' + str(exc), raw=value)
            return
        self.accepted.inc()
        yield canonical(event)


class EventTimestamp(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return timestamp_ms(json.loads(value)['event_time'])


class DeduplicateEnrich(KeyedProcessFunction):
    def __init__(self, ttl_seconds):
        self.ttl_seconds = ttl_seconds

    def open(self, runtime_context: RuntimeContext):
        ttl = (StateTtlConfig.new_builder(Time.seconds(self.ttl_seconds))
               .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
               .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
               .build())
        desc = ValueStateDescriptor('first-source-fingerprint-v1', Types.STRING())
        desc.enable_time_to_live(ttl)
        self.seen = runtime_context.get_state(desc)
        group = runtime_context.get_metrics_group()
        self.clean = group.counter('clean_events')
        self.duplicates = group.counter('duplicate_events')
        self.conflicts = group.counter('conflicting_events')

    def process_element(self, value, ctx):
        event = json.loads(value)
        old = self.seen.value()
        decision, next_value = disposition(old, fingerprint(event))
        if decision == 'clean':
            self.seen.update(next_value)
            self.clean.inc()
            yield canonical(enrich(event))
        elif decision == 'duplicate':
            self.duplicates.inc()
            yield DUPLICATES, audit('exact_duplicate', event=event)
        else:
            self.conflicts.inc()
            yield DLQ, audit('conflicting_payload_for_event_id', event=event)


def kafka_sink(topic, suffix):
    serializer = (KafkaRecordSerializationSchema.builder().set_topic(topic)
                  .set_value_serialization_schema(SimpleStringSchema()).build())
    return (KafkaSink.builder()
            .set_bootstrap_servers(os.getenv('KAFKA_BOOTSTRAP', 'kafka:29092'))
            .set_record_serializer(serializer)
            .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .set_transactional_id_prefix(os.getenv('TX_PREFIX', 'druid-lab-v2') + '-' + suffix)
            .set_property('transaction.timeout.ms', '900000')
            .build())


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    checkpoints = env.get_checkpoint_config()
    checkpoints.set_checkpoint_timeout(120000)
    checkpoints.set_min_pause_between_checkpoints(2000)
    checkpoints.set_max_concurrent_checkpoints(1)
    checkpoints.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    source = (KafkaSource.builder()
              .set_bootstrap_servers(os.getenv('KAFKA_BOOTSTRAP', 'kafka:29092'))
              .set_topics('orders')
              .set_group_id('druid-lab-flink-v2')
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              .set_property('isolation.level', 'read_committed')
              .set_value_only_deserializer(SimpleStringSchema()).build())
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), 'orders-source').uid('orders-source-v1')
    valid = raw.process(Validate(), output_type=Types.STRING()).name('validate-contract').uid('validate-contract-v1')
    watermarks = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
                  .with_timestamp_assigner(EventTimestamp()).with_idleness(Duration.of_seconds(60)))
    timed = valid.assign_timestamps_and_watermarks(watermarks).name('event-time-watermarks').uid('watermarks-v1')
    ttl = int(os.getenv('DEDUP_TTL_SECONDS', '86400'))
    if ttl <= 0:
        raise ValueError('DEDUP_TTL_SECONDS must be positive')
    cleaned = (timed.key_by(lambda x: json.loads(x)['event_id'], key_type=Types.STRING())
               .process(DeduplicateEnrich(ttl), output_type=Types.STRING())
               .name('deduplicate-and-enrich').uid('deduplicate-enrich-v1'))
    cleaned.sink_to(kafka_sink('orders_clean', 'clean')).name('clean-kafka').uid('clean-sink-v1')
    cleaned.get_side_output(DUPLICATES).sink_to(kafka_sink('orders_duplicates', 'duplicates')).name('duplicate-audit-kafka').uid('duplicates-sink-v1')
    invalid = valid.get_side_output(DLQ).union(cleaned.get_side_output(DLQ))
    invalid.sink_to(kafka_sink('orders_dlq', 'dlq')).name('dlq-kafka').uid('dlq-sink-v1')
    env.execute('orders-clean-dedup-enrich-v2')


if __name__ == '__main__':
    main()
