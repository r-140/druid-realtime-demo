"""Offline tests for shared contract/transition logic, not Flink runtime recovery tests."""
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import unittest
from lab.cli import make_events, summarize, check_result

MODULE = Path(__file__).resolve().parents[1] / 'flink' / 'stream_contract.py'
spec = importlib.util.spec_from_file_location('stream_contract', MODULE)
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)
NOW = datetime(2026, 10, 29, 10, tzinfo=timezone.utc)


def event():
    return next(make_events(1, 42, 'flink-test', NOW))


class FlinkContract(unittest.TestCase):
    def test_invalid_json_and_nonobject_go_to_validation_error(self):
        for raw in ['{bad', '[]', 'null', '123', '{"a":NaN}', '{"a":1,"a":2}']:
            with self.subTest(raw=raw), self.assertRaises(contract.ContractError):
                contract.decode_validate(raw)

    def test_money_and_schema_types_are_strict(self):
        cases = [('amount_cents', True), ('amount_cents', '10'), ('amount_cents', 1.5),
                 ('amount_cents', 2**63), ('amount_cents', 0),
                 ('schema_version', True), ('schema_version', 3), ('campaign', 123)]
        for field, value in cases:
            e = event(); e[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(contract.ContractError):
                contract.decode_validate(json.dumps(e))

    def test_timestamp_and_store_validation(self):
        for field, value in [('event_time', '2026-10-29T10:00:00'),
                             ('ingested_at', 'bad-time'), ('store_id', 'unknown'),
                             ('currency', 'EUR'), ('event_type', 'update')]:
            e = event(); e[field] = value
            with self.subTest(field=field), self.assertRaises(contract.ContractError):
                contract.decode_validate(json.dumps(e))
        self.assertEqual(contract.timestamp_ms('2026-10-29T10:00:00Z'),
                         contract.timestamp_ms('2026-10-29T11:00:00+01:00'))

    def test_exact_duplicate_ignores_json_key_order(self):
        e = event()
        reordered = dict(reversed(list(e.items())))
        a = contract.fingerprint(contract.decode_validate(json.dumps(e)))
        b = contract.fingerprint(contract.decode_validate(json.dumps(reordered)))
        self.assertEqual(a, b)
        self.assertEqual(contract.disposition(None, a), ('clean', a))
        self.assertEqual(contract.disposition(a, b), ('duplicate', a))

    def test_conflict_does_not_replace_first_fingerprint(self):
        e = event(); first = contract.fingerprint(e)
        e['amount_cents'] += 1
        self.assertEqual(contract.disposition(first, contract.fingerprint(e)), ('conflict', first))
        # Source payload is immutable, including ingested_at.
        e = event(); e['ingested_at'] = '2026-10-29T10:01:00Z'
        self.assertEqual(contract.disposition(first, contract.fingerprint(e))[0], 'conflict')

    def test_expired_or_missing_state_allows_event_again(self):
        value = contract.fingerprint(event())
        self.assertEqual(contract.disposition(None, value)[0], 'clean')
        # In production None after expiry is supplied by Flink TTL, not this pure function.
        self.assertEqual(contract.disposition(None, value)[0], 'clean')

    def test_enrichment_preserves_money_and_late_events(self):
        e = next(make_events(1, 42, 'late', NOW, late_every=1))
        clean = contract.enrich(contract.decode_validate(json.dumps(e)))
        self.assertEqual(clean['amount_cents'], e['amount_cents'])
        self.assertEqual(clean['event_time'], e['event_time'])
        self.assertEqual(clean['source_delay_ms'], 7200000)
        self.assertEqual(clean['enrichment_version'], 'stores-v1')
        self.assertEqual(clean, contract.enrich(e))

    def test_offline_routes_and_raw_clean_reconciliation(self):
        events = list(make_events(1000, 42, 'acceptance', NOW, late_every=5, duplicate_every=10, schema_v2=True))
        state = {}; clean = []; duplicates = []; dlq = []
        records = [json.dumps(e) for e in events] + ['{malformed-json']*3
        for raw in records:
            try:
                e = contract.decode_validate(raw)
            except contract.ContractError as exc:
                dlq.append(contract.audit('validation: '+str(exc), raw=raw))
                continue
            old = state.get(e['event_id'])
            decision, value = contract.disposition(old, contract.fingerprint(e))
            if decision == 'clean':
                state[e['event_id']] = value
                clean.append(contract.enrich(e))
            elif decision == 'duplicate':
                duplicates.append(e)
        self.assertEqual((len(clean), len(duplicates), len(dlq)), (1000, 100, 3))
        m = summarize(events)
        rows = [{'n':len(clean), 'unique_n':len({e['event_id'] for e in clean}),
                 'amount':sum(e['amount_cents'] for e in clean)}]
        self.assertTrue(check_result(m, rows, unique=True)[0])
        self.assertFalse(check_result(m, rows)[0])
        self.assertEqual(sum(e['source_delay_ms'] == 7200000 for e in clean), 200)

    def test_audit_keeps_payload_and_recoverable_run_id(self):
        e = event(); e['amount_cents'] = 'bad'
        audit = json.loads(contract.audit('validation', raw=json.dumps(e)))
        self.assertEqual(audit['run_id'], e['run_id'])
        self.assertEqual(json.loads(audit['raw']), e)
        self.assertNotIn('run_id', json.loads(contract.audit('malformed', raw='{bad')))

if __name__ == '__main__':
    unittest.main()
