"""Connection-scoped history limits advertised by the authenticated peer."""
import json

DEFAULT_MAX_ROWS = 5000


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate history limit hint')
        result[key] = value
    return result


def login_history_max_rows(payload: bytes) -> int:
    if not 1 < len(payload) <= 4096 or payload[0] != 1:
        return DEFAULT_MAX_ROWS
    try:
        value = json.loads(payload[1:], object_pairs_hook=_unique_object)['history_max_rows']
        if type(value) is int and 0 <= value <= DEFAULT_MAX_ROWS:
            return value
    except (ValueError, TypeError, KeyError, RecursionError):
        pass
    return DEFAULT_MAX_ROWS
