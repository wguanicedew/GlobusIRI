"""PanDA user secrets (optional dependency: pip install "globusiri[panda]").

All token kinds are stored as JSON (the access token file's content). For backward
compatibility, a value that is not JSON is read as a bare access token (that is how
*_IRI_ACCESS was stored before).
"""

import json

from .errors import PandaError


def _client():
    try:
        from pandaclient import Client  # noqa: WPS433
    except Exception as exc:  # pragma: no cover - depends on environment
        raise PandaError('panda-client is not installed; install with: pip install "globusiri[panda]"') from exc
    return Client


def set_secret(key, payload):
    if not key:
        raise PandaError("no panda_secret_key configured for this token")
    status, result = _client().set_user_secret(key, json.dumps(payload))
    success, message = result if isinstance(result, (list, tuple)) else (False, result)
    if status != 0 or not success:
        raise PandaError(f"failed to set PanDA secret '{key}': status={status} message={message}")


def get_secret(key):
    """Return the decoded payload dict for key."""
    if not key:
        raise PandaError("no panda_secret_key configured for this token")
    status, result = _client().get_user_secrets()
    success, secrets = result if isinstance(result, (list, tuple)) else (False, result)
    if status != 0 or not success:
        raise PandaError(f"failed to get PanDA secrets: status={status}")
    raw = (secrets or {}).get(key)
    if not raw:
        raise PandaError(f"PanDA secret '{key}' is empty")
    return decode(raw)


def decode(raw):
    if isinstance(raw, dict):
        return raw
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {"access_token": str(raw).strip()}
    if isinstance(data, dict):
        return data
    return {"access_token": str(raw).strip()}
