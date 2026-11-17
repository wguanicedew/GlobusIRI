#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Shared HTTP helpers: retries with backoff, and access-token sources read from disk."""

import logging
import os
import time

import requests

from ..errors import LoginRequired, RemoteError
from ..secure_io import read_yaml

log = logging.getLogger("globusiri.http")

RETRY_STATUS = {429, 500, 502, 503, 504}
DEFAULT_TIMEOUT = 120


def request_with_retry(session, method, url, *, retries=3, backoff=2.0, data_factory=None, **kwargs):
    """Send a request, retrying only on connection errors and HTTP 429/5xx.

    ``data_factory`` (callable returning a fresh body, e.g. an open file) is used for
    bodies that can only be read once, so each attempt gets a new one.
    """
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    attempt = 0
    while True:
        attempt += 1
        body = None
        try:
            if data_factory is not None:
                body = data_factory()
                kwargs["data"] = body
            resp = session.request(method, url, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt > retries:
                raise RemoteError(f"{method} {url} failed after {attempt} attempts: {exc}") from exc
            log.debug("%s %s: %s; retry %d/%d", method, url, exc, attempt, retries)
        else:
            if resp.status_code not in RETRY_STATUS or attempt > retries:
                return resp
            log.debug("%s %s -> HTTP %s; retry %d/%d", method, url, resp.status_code, attempt, retries)
        finally:
            if body is not None and hasattr(body, "close"):
                body.close()
        time.sleep(min(backoff ** attempt, 30))


class TokenSource:
    """Reads an access token from a YAML token file, re-reading it when the file changes.

    Expired or missing tokens raise LoginRequired with the command that fixes them.
    """

    def __init__(self, path, *, label, fix):
        self.path = path
        self.label = label
        self.fix = fix
        self._mtime = None
        self._data = {}

    def data(self):
        try:
            mtime = os.path.getmtime(self.path)
        except OSError:
            raise LoginRequired(f"{self.label} access token file {self.path} not found", fix=self.fix) from None
        if mtime != self._mtime:
            self._data = read_yaml(self.path)
            self._mtime = mtime
        return self._data

    def token(self):
        data = self.data()
        tok = data.get("access_token")
        if not tok:
            raise LoginRequired(f"{self.label} access token file {self.path} has no access_token", fix=self.fix)
        exp = data.get("expires_at")
        try:
            if exp and float(exp) < time.time():
                raise LoginRequired(f"{self.label} access token expired", fix=self.fix)
        except (TypeError, ValueError):
            pass
        return tok
