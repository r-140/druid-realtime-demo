"""Small, explicit clients for the lab. Run from the project root."""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import random
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4, uuid5, NAMESPACE_URL

ROOT = Path(__file__).resolve().parents[1]
ROUTER = os.getenv('DRUID_URL', 'http://localhost:8888')
OVERLORD = os.getenv('OVERLORD_URL', 'http://localhost:8081')


def iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def parse_time(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timestamp must include a timezone')
    return result.astimezone(timezone.utc)


def request(base, path, body=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = Request(base + path, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urlopen(req, timeout=timeout) as response:
            payload = response.read()
            return json.loads(payload) if payload else None
    except HTTPError as exc:
        raise RuntimeError(f'HTTP {exc.code} {path}: {exc.read().decode()[:2000]}') from exc


def sql(query):
    return request(ROUTER, '/druid/v2/sql', {'query': query, 'context': {'timeout': 20000}})


def identifier(value):
    if not re.fullmatch(r'[a-z][a-z0-9_]*', value):
        raise ValueError('Use a lowercase SQL identifier')
    return value


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def make_events(count, seed, run_id, now, late_every=0, duplicate_every=0, schema_v2=False):
    rng = random.Random(seed)
    for i in range(count):
        kind = 'refund' if i % 10 == 9 else 'sale'
        cents = rng.randint(100, 50000) * (-1 if kind == 'refund' else 1)
        event = {
            'event_id': str(uuid5(NAMESPACE_URL, f'{run_id}/{i}')),
            'order_id': f'{run_id}-{i}', 'run_id': run_id,
            'event_time': iso(now - timedelta(hours=2) if late_every and (i + 1) % late_every == 0 else now),
            'ingested_at': iso(now), 'store_id': f'store-{rng.randrange(5)}',
            'country': 'PL', 'channel': rng.choice(['web', 'app', 'store']),
            'event_type': kind, 'currency': 'PLN', 'amount_cents': cents,
            'schema_version': 2 if schema_v2 else 1,
        }
        if schema_v2:
            event['campaign'] = rng.choice(['autumn', 'organic'])
        yield event
        if duplicate_every and (i + 1) % duplicate_every == 0:
            yield dict(event)


def summarize(events):
    counts = Counter(e['event_id'] for e in events)
    unique = {e['event_id']: e for e in events}
    return {'valid_records': len(events), 'unique_events': len(unique),
            'duplicate_records': sum(n - 1 for n in counts.values()),
            'amount_cents': sum(e['amount_cents'] for e in events),
            'unique_amount_cents': sum(e['amount_cents'] for e in unique.values())}


def generate(args):
    now = parse_time(args.base_time) if args.base_time else datetime.now(timezone.utc)
    run_id = args.run_id or uuid4().hex[:12]
    events = list(make_events(args.count, args.seed, run_id, now, args.late_every, args.duplicate_every, args.schema_v2))
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    # One archive per run; refuse accidental replacement of an existing ledger.
    with path.open('x') as stream:
        for e in events:
            stream.write(json.dumps(e) + '\n')
        for _ in range(args.bad_records):
            stream.write('{malformed-json\n')
    manifest = {'run_id': run_id, 'base_time': iso(now), 'seed': args.seed,
                'bad_records': args.bad_records, **summarize(events)}
    Path(str(path) + '.manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(manifest, indent=2))


def bootstrap(args):
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaException, KafkaError
    admin = AdminClient({'bootstrap.servers': args.bootstrap})
    result = admin.create_topics([NewTopic('orders', num_partitions=3, replication_factor=1,
                                          config={'retention.ms': '604800000'})])
    for topic, future in result.items():
        try:
            future.result(timeout=60)
            print(f'Created {topic}')
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            print(f'{topic} exists; configuration has not been changed')


def send(args):
    from confluent_kafka import Producer
    producer = Producer({'bootstrap.servers': args.bootstrap, 'enable.idempotence': True,
                         'acks': 'all', 'delivery.timeout.ms': 60000})
    errors = []
    delivered = 0
    def callback(err, msg):
        nonlocal delivered
        if err:
            errors.append(str(err))
        else:
            delivered += 1
    try:
        with Path(args.file).open('rb') as stream:
            for line in stream:
                try:
                    event = json.loads(line)
                    key = str(event['order_id']).encode()
                except (ValueError, KeyError, TypeError):
                    key = b'bad-event'
                while True:
                    try:
                        producer.produce('orders', key=key, value=line.rstrip(b'\n'), on_delivery=callback)
                        break
                    except BufferError:
                        producer.poll(0.1)
                producer.poll(0)
                if args.rate:
                    time.sleep(1 / args.rate)
    finally:
        remaining = producer.flush(70)
    if errors or remaining:
        raise RuntimeError(f'Failed deliveries: {errors[:5]}; still buffered: {remaining}. Reconcile before replaying.')
    print(f'Acknowledged {delivered} Kafka records. Resending the file creates business duplicates.')


def ingest(args):
    print(json.dumps(request(OVERLORD, '/druid/indexer/v1/supervisor', json.loads(Path(args.spec).read_text())), indent=2))


def check_result(manifest, rows, rollup=False):
    row = rows[0]
    expected = {'n': manifest['valid_records'], 'amount': manifest['amount_cents']}
    if not rollup:
        expected['unique_n'] = manifest['unique_events']
    actual = {key: int(row.get(key) or 0) for key in expected}
    return actual == expected, {'expected': expected, 'actual': actual}


def verify(args):
    m = json.loads(Path(args.manifest).read_text())
    ds = identifier(args.datasource)
    rollup = args.rollup
    query = (f'SELECT SUM(events) AS n, SUM(amount_cents_sum) AS amount FROM {ds}' if rollup else
             f'SELECT COUNT(*) AS n, COUNT(DISTINCT event_id) AS unique_n, SUM(amount_cents) AS amount FROM {ds}')
    query += f' WHERE run_id = {literal(m["run_id"])}'
    # Explicit exact distinct for deterministic validation; approximate sketches are a separate exercise.
    deadline = time.monotonic() + args.timeout
    last = {}
    while True:
        try:
            rows = request(ROUTER, '/druid/v2/sql', {'query': query, 'context': {'useApproximateCountDistinct': False, 'timeout': 20000}})
            good, last = check_result(m, rows, rollup)
            if good:
                print(json.dumps(last, indent=2))
                return
        except (RuntimeError, URLError, TimeoutError) as exc:
            last = {'error': str(exc)}
        if time.monotonic() >= deadline:
            raise RuntimeError(f'Reconciliation did not pass: {json.dumps(last)}')
        time.sleep(3)


def wait(args):
    deadline = time.monotonic() + args.timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if sql('SELECT 1 AS ready') == [{'ready': 1}]:
                request(OVERLORD, '/druid/indexer/v1/supervisor')
                print('SQL and Overlord APIs ready; ingestion task startup may take longer.')
                return
        except (RuntimeError, URLError, TimeoutError) as exc:
            last = str(exc)
        time.sleep(3)
    raise RuntimeError(f'Cluster not ready: {last}')


def backfill(args):
    """Rebuild into a different datasource; never mutate a streaming datasource."""
    file = Path(args.file).resolve()
    if file.parent != (ROOT / 'data').resolve():
        raise ValueError('Backfill input must be directly in project data/ (mounted into workers)')
    events = []
    bad = 0
    for line in file.read_text().splitlines():
        try:
            e = json.loads(line)
            parse_time(e['event_time'])
            if not isinstance(e.get('amount_cents'), int):
                raise ValueError('Invalid cents')
            events.append(e)
        except (ValueError, TypeError, KeyError):
            bad += 1
    if not events:
        raise ValueError('No valid events')
    # Canonical ledger: first-seen event wins. Conflicting payloads must be resolved by a real contract.
    canonical = {}
    for e in events:
        old = canonical.get(e['event_id'])
        if old is not None and old != e:
            raise ValueError('Conflicting payloads for event_id; cannot silently choose a winner')
        canonical[e['event_id']] = e
    ds = identifier(args.datasource)
    if not ds.startswith('orders_rebuilt_'):
        raise ValueError('Use a new orders_rebuilt_<suffix> datasource for each rebuild')
    if sql(f'SELECT COUNT(*) AS n FROM sys.segments WHERE datasource = {literal(ds)}')[0]['n']:
        raise ValueError('Datasource already has segments; choose a new suffix')
    target = ROOT / 'data' / f'{ds}.jsonl'
    with target.open('x') as stream:
        for e in canonical.values():
            stream.write(json.dumps(e) + '\n')
    times = [parse_time(e['event_time']) for e in canonical.values()]
    start = min(times).replace(minute=0, second=0, microsecond=0)
    end = max(times).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    schema = json.loads((ROOT / 'specs/orders_raw.json').read_text())['spec']['dataSchema']
    schema['dataSource'] = ds
    schema['granularitySpec']['intervals'] = [iso(start) + '/' + iso(end)]
    task = {'type':'index_parallel','spec':{'dataSchema':schema,
        'ioConfig':{'type':'index_parallel','inputSource':{'type':'local','baseDir':'/opt/lab/data','filter':target.name},'inputFormat':{'type':'json'},'appendToExisting':False},
        'tuningConfig':{'type':'index_parallel','maxNumConcurrentSubTasks':1,'maxParseExceptions':0}}}
    print(f'Dropped {bad} malformed lines, removed {len(events)-len(canonical)} exact duplicates.')
    print(json.dumps(request(OVERLORD, '/druid/indexer/v1/task', task), indent=2))
    print(f'Reconcile {ds} against unique_events and unique_amount_cents in the source manifest after task SUCCESS and segment availability.')


def compact(args):
    if args.datasource not in ['orders_raw', 'orders_minute']:
        raise ValueError('Compaction limited to lab datasources')
    bounds = args.interval.split('/')
    if len(bounds) != 2 or parse_time(bounds[0]) >= parse_time(bounds[1]):
        raise ValueError('Expected a nonempty ISO timestamp interval')
    body = {'type':'compact','dataSource':args.datasource,'ioConfig':{'type':'compact','inputSpec':{'type':'interval','interval':args.interval}},'tuningConfig':{'type':'index_parallel','maxNumConcurrentSubTasks':1}}
    print(json.dumps(request(OVERLORD, '/druid/indexer/v1/task', body), indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    g = sub.add_parser('generate')
    g.add_argument('--count', type=int, default=1000)
    g.add_argument('--seed', type=int, default=42)
    g.add_argument('--run-id')
    g.add_argument('--base-time')
    g.add_argument('--late-every', type=int, default=0)
    g.add_argument('--duplicate-every', type=int, default=0)
    g.add_argument('--bad-records', type=int, default=0)
    g.add_argument('--schema-v2', action='store_true')
    g.add_argument('--output', required=True)
    g.set_defaults(func=generate)
    for name, func in [('bootstrap', bootstrap), ('send', send)]:
        c = sub.add_parser(name)
        c.add_argument('--bootstrap', default='localhost:9092')
        if name == 'send':
            c.add_argument('file')
            c.add_argument('--rate', type=float, default=0, help='Records/sec; zero sends without throttling')
        c.set_defaults(func=func)
    c = sub.add_parser('ingest'); c.add_argument('spec'); c.set_defaults(func=ingest)
    c = sub.add_parser('query'); c.add_argument('file'); c.set_defaults(func=lambda a: print(json.dumps(sql(Path(a.file).read_text()), indent=2)))
    c = sub.add_parser('status'); c.add_argument('--supervisor', default='orders_raw', type=identifier)
    c.set_defaults(func=lambda a: print(json.dumps(request(OVERLORD, f'/druid/indexer/v1/supervisor/{a.supervisor}/status'), indent=2)))
    c = sub.add_parser('wait'); c.add_argument('--timeout', type=int, default=300); c.set_defaults(func=wait)
    c = sub.add_parser('verify'); c.add_argument('manifest'); c.add_argument('--datasource', default='orders_raw'); c.add_argument('--rollup', action='store_true'); c.add_argument('--timeout', type=int, default=180); c.set_defaults(func=verify)
    c = sub.add_parser('backfill'); c.add_argument('file'); c.add_argument('--datasource', required=True); c.set_defaults(func=backfill)
    c = sub.add_parser('compact'); c.add_argument('--datasource', default='orders_raw'); c.add_argument('--interval', required=True); c.set_defaults(func=compact)
    args = parser.parse_args()
    for field in ['count', 'late_every', 'duplicate_every', 'bad_records', 'rate']:
        if hasattr(args, field) and getattr(args, field) < 0:
            parser.error(f'{field} must be nonnegative')
    if args.command == 'generate' and args.count == 0:
        parser.error('count must be positive')
    try:
        args.func(args)
    except (RuntimeError, ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
