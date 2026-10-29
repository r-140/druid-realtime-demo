"""Wait for checkpoint-committed Flink source offsets to reach a fixed Kafka snapshot.

This observes progress only; it does not prove sink commit or Druid availability.
"""
import argparse
import json
import time


def main():
    from confluent_kafka import Consumer, TopicPartition
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--group', default='druid-lab-flink-v2')
    parser.add_argument('--topic', default='orders')
    parser.add_argument('--timeout', type=int, default=300)
    args = parser.parse_args()
    consumer = Consumer({'bootstrap.servers':args.bootstrap, 'group.id':args.group,
                         'enable.auto.commit':False, 'allow.auto.create.topics':False})
    try:
        metadata = consumer.list_topics(args.topic, timeout=15).topics[args.topic]
        if metadata.error:
            raise RuntimeError(str(metadata.error))
        partitions = [TopicPartition(args.topic, n) for n in sorted(metadata.partitions)]
        targets = {tp.partition: consumer.get_watermark_offsets(tp, timeout=15)[1] for tp in partitions}
        print('Fixed source end-offset snapshot:', json.dumps(targets), flush=True)
        deadline = time.monotonic() + args.timeout
        last = {}
        while True:
            offsets = consumer.committed(partitions, timeout=15)
            if any(tp.error for tp in offsets):
                raise RuntimeError(str([str(tp.error) for tp in offsets if tp.error]))
            last = {tp.partition:tp.offset for tp in offsets}
            # Empty partitions need no committed offset. Nonempty ones must reach the snapshot.
            if all(end == 0 or last.get(n, -1) >= end for n,end in targets.items()):
                print('Checkpoint-committed source offsets caught up:', json.dumps(last))
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(f'Source progress timeout: targets={targets}, committed={last}')
            time.sleep(3)
    finally:
        # No subscribe/assign/poll: this observer never joins the group or changes its offsets.
        consumer.close()

if __name__ == '__main__':
    main()
