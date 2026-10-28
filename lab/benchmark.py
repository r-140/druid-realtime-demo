"""Repeat a SELECT query and print observed wall-clock latency, without an SLA claim."""
import argparse
import json
import math
from pathlib import Path
import statistics
import time
from lab.cli import sql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('query_file')
    parser.add_argument('--iterations', type=int, default=30)
    args = parser.parse_args()
    if args.iterations < 2:
        parser.error('Need at least two samples')
    query = Path(args.query_file).read_text()
    sql(query)  # Warm-up excluded.
    samples = []
    for _ in range(args.iterations):
        started = time.perf_counter()
        sql(query)
        samples.append((time.perf_counter() - started) * 1000)
    samples.sort()
    print(json.dumps({'samples': len(samples), 'p50_ms': statistics.median(samples),
        'p95_ms': samples[math.ceil(.95 * len(samples)) - 1], 'max_ms': max(samples)}, indent=2))

if __name__ == '__main__':
    main()
