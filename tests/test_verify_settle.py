"""Ensure an early matching query cannot mask a later duplicate during settling."""
import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from lab.cli import verify


class VerifySettling(unittest.TestCase):
    def test_transient_match_followed_by_duplicates_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / 'manifest.json'
            p.write_text(json.dumps({'run_id':'settle', 'valid_records':2, 'unique_events':1,
                                     'amount_cents':200, 'unique_amount_cents':100}))
            args = argparse.Namespace(manifest=str(p), datasource='orders_clean', rollup=False,
                                      unique=True, timeout=9, settle_seconds=6)
            now = [0]
            calls = [0]
            def query(*args, **kwargs):
                calls[0] += 1
                return [{'n':1 if calls[0] == 1 else 2, 'unique_n':1,
                         'amount':100 if calls[0] == 1 else 200}]
            def advance(seconds):
                now[0] += seconds
            with patch('lab.cli.request', side_effect=query), \
                 patch('lab.cli.time.monotonic', side_effect=lambda:now[0]), \
                 patch('lab.cli.time.sleep', side_effect=advance):
                with self.assertRaisesRegex(RuntimeError, 'Reconciliation did not pass'):
                    verify(args)
            self.assertGreater(calls[0], 1)

if __name__ == '__main__':
    unittest.main()
