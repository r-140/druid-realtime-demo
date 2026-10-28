"""Application-level Prometheus probe, not a replacement for native Druid metrics."""
import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import URLError
from lab.cli import sql


def collect():
    started = time.monotonic()
    try:
        sql('SELECT 1 AS ready')
        up = 1
    except (RuntimeError, OSError):
        up = 0
    latency = time.monotonic() - started
    fresh = -1
    freshness_up = 0
    if up:
        try:
            rows = sql("SELECT MAX(ingested_at) AS latest FROM orders_raw")
            if rows and rows[0].get('latest'):
                from lab.cli import parse_time
                fresh = max(0, time.time() - parse_time(rows[0]['latest']).timestamp())
                freshness_up = 1
        except (RuntimeError, OSError, ValueError):
            pass
    return '\n'.join([
        '# HELP lab_druid_sql_up SQL SELECT 1 succeeded.',
        '# TYPE lab_druid_sql_up gauge', f'lab_druid_sql_up {up}',
        '# HELP lab_druid_sql_probe_seconds Latest SELECT 1 latency; errors include timeout duration.',
        '# TYPE lab_druid_sql_probe_seconds gauge', f'lab_druid_sql_probe_seconds {latency:.6f}',
        '# HELP lab_freshness_probe_up Freshness query returned a timestamp.',
        '# TYPE lab_freshness_probe_up gauge', f'lab_freshness_probe_up {freshness_up}',
        '# HELP lab_latest_produced_event_age_seconds Age of latest visible ingested_at; -1 means unavailable. Not Kafka offset lag.',
        '# TYPE lab_latest_produced_event_age_seconds gauge', f'lab_latest_produced_event_age_seconds {fresh:.3f}',
        '# HELP lab_probe_sample_timestamp_seconds Collection completion time.',
        '# TYPE lab_probe_sample_timestamp_seconds gauge', f'lab_probe_sample_timestamp_seconds {time.time():.3f}', ''
    ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9108)
    args = parser.parse_args()
    state = {'body': '# Probe initializing\n'}
    def loop():
        while True:
            state['body'] = collect()
            time.sleep(15)
    threading.Thread(target=loop, daemon=True).start()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != '/metrics':
                self.send_error(404)
                return
            body = state['body'].encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args):
            pass
    print(f'Probe listening at http://{args.host}:{args.port}/metrics', flush=True)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()

if __name__ == '__main__':
    main()
