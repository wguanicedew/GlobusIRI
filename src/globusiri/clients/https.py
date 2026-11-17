"""Globus HTTPS client: PUT/GET files on one collection's https_server.

Paths given to this client are HPC mount paths; they are translated to collection
paths with the collection's PathMap before building the URL.

Besides the quick retries of request_with_retry (connection errors, HTTP 429/5xx), a failed
GET or PUT is tried again after `retry_sleep` seconds, up to `retry_attempts` attempts in all.
That covers errors that need time rather than an immediate resend, such as a file written at
the end of a job that the collection does not see yet (HTTP 404 "No such file or directory"),
or a transient GlobusError. 401/403 (token problems) are never retried.
"""

import logging
import os
import time
from urllib.parse import quote

import requests

from ..errors import LoginRequired, RemoteError
from .http import request_with_retry

log = logging.getLogger("globusiri.https")


class GlobusHttpsClient:
    def __init__(self, token_source, path_map, *, retries=3, debug=False, https_server=None,
                 retry_attempts=3, retry_sleep=10):
        self.token_source = token_source
        self.path_map = path_map
        self.retries = retries
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_sleep = retry_sleep
        self.debug = debug
        self._https_server = https_server
        self.session = requests.Session()

    @property
    def https_server(self):
        server = self._https_server or self.token_source.data().get("https_server")
        if not server:
            raise LoginRequired(f"no https_server in {self.token_source.path}", fix=self.token_source.fix)
        return str(server).rstrip("/")

    def url(self, hpc_path):
        globus_path = self.path_map.to_globus(hpc_path)
        return f"{self.https_server}/{quote(globus_path.lstrip('/'))}"

    def _headers(self):
        return {"Authorization": f"Bearer {self.token_source.token()}"}

    def _check(self, resp, what, url):
        if resp.ok:
            return
        detail = resp.text[:300] if resp.text else resp.reason
        if resp.status_code in (401, 403):
            raise LoginRequired(f"HTTPS {what} {url}: HTTP {resp.status_code}: {detail}", fix=self.token_source.fix)
        raise RemoteError(f"HTTPS {what} {url}: HTTP {resp.status_code}: {detail}", status=resp.status_code)

    def _with_retries(self, what, url, send):
        """send(), tried again after retry_sleep while it raises RemoteError (not LoginRequired)."""
        for attempt in range(1, self.retry_attempts + 1):
            try:
                return send()
            except RemoteError as exc:
                if attempt >= self.retry_attempts:
                    if self.retry_attempts > 1:
                        raise RemoteError(f"{exc} (failed {attempt} times)", status=exc.status) from None
                    raise
                log.warning("HTTPS %s %s failed (attempt %d/%d): %s; retrying in %ss",
                            what, url, attempt, self.retry_attempts, str(exc).splitlines()[0][:200], self.retry_sleep)
                time.sleep(self.retry_sleep)

    def put(self, local_file, hpc_path):
        url = self.url(hpc_path)
        self._with_retries("PUT", url, lambda: self._put(local_file, url))

    def _put(self, local_file, url):
        if self.debug:
            log.debug("curl -s -X PUT -H 'Authorization: Bearer <redacted>' -T %s %s", local_file, url)
        resp = request_with_retry(self.session, "PUT", url, retries=self.retries, headers=self._headers(),
                                  data_factory=lambda: open(local_file, "rb"))
        self._check(resp, "PUT", url)

    def get(self, hpc_path, local_file):
        url = self.url(hpc_path)
        self._with_retries("GET", url, lambda: self._get(url, local_file))

    def _get(self, url, local_file):
        if self.debug:
            log.debug("curl -s -H 'Authorization: Bearer <redacted>' -o %s %s", local_file, url)
        resp = request_with_retry(self.session, "GET", url, retries=self.retries, headers=self._headers(), stream=True)
        self._check(resp, "GET", url)
        os.makedirs(os.path.dirname(os.path.abspath(local_file)), exist_ok=True)
        tmp = f"{local_file}.part"
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                fh.write(chunk)
        os.replace(tmp, local_file)

    def probe(self, hpc_path):
        """GET a path without raising; return (status_code, url). Used by validation."""
        url = self.url(hpc_path)
        resp = request_with_retry(self.session, "GET", url, retries=self.retries, headers=self._headers(), stream=True)
        resp.close()
        return resp.status_code, url
