#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""PanDA user secrets (optional dependency: pip install "globusiri[panda]").

All token kinds are stored as JSON (the access token file's content). For backward
compatibility, a value that is not JSON is read as a bare access token (that is how
*_IRI_ACCESS was stored before).
"""

import json

from .errors import PandaError

_client_override = None


def set_client(client):
    """Send PanDA secret requests through `client` instead of pandaclient.Client (which takes
    the server URL and credentials from PANDA_URL_SSL, X509_USER_PROXY, PANDA_AUTH, ...).
    An embedding application (e.g. Harvester) passes its own PanDA connection here.

    client must have pandaclient.Client's interface:
      set_user_secret(key, value) -> (status, (success, message))
      get_user_secrets()          -> (status, (success, {key: value}))
    status 0 means the server was reached. None restores pandaclient.Client."""
    global _client_override
    _client_override = client


def _client():
    if _client_override is not None:
        return _client_override
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
        raise PandaError(f"failed to get PanDA secrets: status={status} message={secrets}")
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
