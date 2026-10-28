import json
from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest.mock import patch
from lab.cli import make_events, summarize, check_result, literal, identifier, parse_time
from lab.probe import collect

NOW = datetime(2026, 10, 28, 10, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]


class EventContract(unittest.TestCase):
    def test_repeatable_generation_and_integer_money(self):
        a = list(make_events(100, 42, 'test', NOW))
        self.assertEqual(a, list(make_events(100, 42, 'test', NOW)))
        self.assertTrue(all(type(e['amount_cents']) is int for e in a))
        self.assertTrue(all((e['amount_cents'] < 0) == (e['event_type'] == 'refund') for e in a))
        self.assertEqual(len({e['event_id'] for e in a}), 100)

    def test_duplicates_and_late_events_preserve_ledger(self):
        events = list(make_events(100, 42, 'test', NOW, late_every=5, duplicate_every=10))
        manifest = summarize(events)
        self.assertEqual(manifest['valid_records'], 110)
        self.assertEqual(manifest['unique_events'], 100)
        self.assertEqual(manifest['duplicate_records'], 10)
        self.assertEqual(sum(parse_time(e['event_time']) < NOW for e in events), 30)
        self.assertEqual(manifest['amount_cents'], sum(e['amount_cents'] for e in events))

    def test_schema_evolution_is_additive(self):
        old = list(make_events(1, 42, 'test', NOW))[0]
        new = list(make_events(1, 42, 'test', NOW, schema_v2=True))[0]
        self.assertNotIn('campaign', old)
        self.assertIn('campaign', new)
        for k in old.keys() - {'schema_version'}:
            self.assertEqual(old[k], new[k])

    def test_reconciliation_detects_loss_duplicate_and_wrong_sum(self):
        m = summarize(list(make_events(10, 42, 'test', NOW)))
        good = {'n': m['valid_records'], 'unique_n': m['unique_events'], 'amount': m['amount_cents']}
        self.assertTrue(check_result(m, [good])[0])
        for key in good:
            bad = dict(good); bad[key] += 1
            self.assertFalse(check_result(m, [bad])[0])
        self.assertTrue(check_result(m, [good], rollup=True)[0])

    def test_sql_escaping(self):
        self.assertEqual(literal("a'b"), "'a''b'")
        with self.assertRaises(ValueError):
            identifier('orders;DROP TABLE x')
        with self.assertRaises(ValueError):
            parse_time('2026-10-28T10:00:00')

    def test_rollup_keeps_only_bounded_dimensions_and_has_count(self):
        raw = json.loads((ROOT/'specs/orders_raw.json').read_text())['spec']['dataSchema']
        roll = json.loads((ROOT/'specs/orders_minute.json').read_text())['spec']['dataSchema']
        self.assertFalse(raw['granularitySpec']['rollup'])
        self.assertTrue(roll['granularitySpec']['rollup'])
        names = {d['name'] for d in roll['dimensionsSpec']['dimensions']}
        self.assertNotIn('event_id', names)
        self.assertNotIn('ingested_at', names)
        self.assertEqual(roll['metricsSpec'][0], {'type':'count','name':'events'})
        self.assertEqual(roll['metricsSpec'][1]['fieldName'], 'amount_cents')

    def test_probe_distinguishes_sql_health_from_missing_data(self):
        with patch('lab.probe.sql', side_effect=[[{'ready':1}], RuntimeError('missing datasource')]):
            result = collect()
        self.assertIn('lab_druid_sql_up 1', result)
        self.assertIn('lab_freshness_probe_up 0', result)
        self.assertIn('lab_latest_produced_event_age_seconds -1', result)
        with patch('lab.probe.sql', side_effect=RuntimeError('unavailable')):
            self.assertIn('lab_druid_sql_up 0', collect())

if __name__ == '__main__':
    unittest.main()
