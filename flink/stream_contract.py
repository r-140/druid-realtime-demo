"""Pure event contract shared by the Flink operators and offline tests."""
from datetime import datetime, timezone
import hashlib
import json

STORES = {
    'store-0': ('Krakow', 'south'),
    'store-1': ('Warsaw', 'central'),
    'store-2': ('Gdansk', 'north'),
    'store-3': ('Wroclaw', 'west'),
    'store-4': ('Lublin', 'east'),
}
ENRICHMENT_VERSION = 'stores-v1'


class ContractError(ValueError):
    pass


def timestamp_ms(value):
    if not isinstance(value, str):
        raise ContractError('timestamp must be an ISO string')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError('timezone missing')
        # Integer arithmetic preserves millisecond precision without float rounding.
        delta = parsed.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
        return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000
    except (ValueError, OverflowError) as exc:
        raise ContractError('timestamp must be valid ISO time with a timezone') from exc


def reject_constant(value):
    raise ContractError(f'Nonstandard JSON constant: {value}')


def no_repeated_keys(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ContractError(f'Repeated JSON key: {key}')
        obj[key] = value
    return obj


def decode_validate(raw):
    try:
        event = json.loads(raw, parse_constant=reject_constant, object_pairs_hook=no_repeated_keys)
    except (ValueError, TypeError) as exc:
        raise ContractError(f'Invalid JSON: {exc}') from exc
    if not isinstance(event, dict):
        raise ContractError('Expected a JSON object')
    for name in ['event_id', 'order_id', 'run_id', 'store_id', 'country', 'channel',
                 'event_type', 'currency', 'event_time', 'ingested_at']:
        if not isinstance(event.get(name), str) or not event[name].strip():
            raise ContractError(f'{name} must be a nonempty string')
    if event['store_id'] not in STORES:
        raise ContractError('Unknown store_id')
    if event['country'] != 'PL' or event['currency'] != 'PLN':
        raise ContractError('This learning contract supports PL/PLN only')
    if event['channel'] not in ['web', 'app', 'store']:
        raise ContractError('Unsupported channel')
    if type(event.get('amount_cents')) is not int or not -(2**63) <= event['amount_cents'] < 2**63:
        raise ContractError('amount_cents must be a signed 64-bit integer, not float/bool/string')
    if event['event_type'] not in ['sale', 'refund']:
        raise ContractError('Unsupported event_type')
    if (event['event_type'] == 'sale' and event['amount_cents'] <= 0) or (
            event['event_type'] == 'refund' and event['amount_cents'] >= 0):
        raise ContractError('Sale must be positive and refund negative')
    if type(event.get('schema_version')) is not int or event['schema_version'] not in [1, 2]:
        raise ContractError('schema_version must be integer 1 or 2')
    if event.get('campaign') is not None and not isinstance(event['campaign'], str):
        raise ContractError('campaign must be a string or null')
    timestamp_ms(event['event_time'])
    timestamp_ms(event['ingested_at'])
    return event


def canonical(event):
    return json.dumps(event, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def fingerprint(event):
    # Full source payload is immutable: even an ingested_at change is a conflict.
    return hashlib.sha256(canonical(event).encode()).hexdigest()


def disposition(previous, current):
    """Return the disposition and next fingerprint. Only new IDs update state."""
    if previous is None:
        return 'clean', current
    return ('duplicate' if previous == current else 'conflict'), previous


def enrich(event):
    result = dict(event)
    city, region = STORES[event['store_id']]
    result.update(store_city=city, store_region=region, enrichment_version=ENRICHMENT_VERSION,
                  source_delay_ms=timestamp_ms(event['ingested_at']) - timestamp_ms(event['event_time']))
    return result


def audit(reason, raw=None, event=None):
    result = {'reason': reason}
    if event is not None:
        result.update(run_id=event['run_id'], event_id=event['event_id'], original=event)
    else:
        result['raw'] = raw
        # Recover run identity for structurally valid but contract-invalid records.
        try:
            candidate = json.loads(raw)
            if isinstance(candidate, dict) and isinstance(candidate.get('run_id'), str):
                result['run_id'] = candidate['run_id']
        except (ValueError, TypeError):
            pass
    return canonical(result)
