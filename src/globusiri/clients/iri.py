#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""IRI API client (ported from harvester iri_utils.IriClient).

Differences from the Harvester version: the access token comes from a TokenSource
(re-read when the token file changes), requests retry on connection errors and HTTP
429/5xx, and debug output never contains the bearer token.

NERSC's IRI sometimes fails a call with "Unable to get sfapi data: 403" (as the HTTP error,
or as the result of the call's failed task), and the same call works moments later. Such a
call is sent again after `sfapi_retry_sleep` seconds; it fails (SfapiUnavailable) only when
`sfapi_retries` attempts in a row get the error.

Other failed calls (an HTTP error after the quick retries, or a failed task) are sent again
after `retry_sleep` seconds, up to `retry_attempts` attempts in all, except:

* definite answers, which another attempt would only repeat: HTTP 404 and "No such file or
  directory" (a missing path; cleanup relies on it), and HTTP 501 (not implemented);
* job submission, where a lost reply may hide an accepted job: retrying could start the job
  twice (the workflow's submit step has its own attempts, and a job is never duplicated
  by the client);
* 401/403 token problems (LoginRequired).
"""

import json
import logging
import os
import shlex
import time
from urllib.parse import quote, urlencode

import requests

from ..errors import LoginRequired, RemoteError
from .http import request_with_retry

log = logging.getLogger("globusiri.iri")

_TASK_TERMINAL_STATES = {"completed", "failed", "canceled"}
SFAPI_ERROR = "Unable to get sfapi data"


class SfapiUnavailable(RemoteError):
    """NERSC's transient "Unable to get sfapi data" error (raised after the retries)."""


def _is_sfapi(detail):
    return SFAPI_ERROR in str(detail)


def _retryable(exc):
    """A RemoteError worth another attempt (see the module docstring)."""
    return exc.status not in (404, 501) and "No such file or directory" not in str(exc)


def _encode(segment):
    return quote(str(segment), safe="")


class IriClient:
    def __init__(self, base_url, token_source, *, resource_id=None, retries=3, debug=False,
                 task_poll_interval=5, task_max_polls=60, sfapi_retries=3, sfapi_retry_sleep=3,
                 retry_attempts=3, retry_sleep=3):
        self.base_url = base_url.rstrip("/")
        self.token_source = token_source
        self.resource_id = resource_id
        self.retries = retries
        self.debug = debug
        self.task_poll_interval = task_poll_interval
        self.task_max_polls = task_max_polls
        self.sfapi_retries = max(1, int(sfapi_retries))
        self.sfapi_retry_sleep = sfapi_retry_sleep
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_sleep = retry_sleep
        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"

    # ------------------------------------------------------------------ core
    def _rid(self, resource_id):
        rid = resource_id or self.resource_id
        if not rid:
            raise RemoteError("IRI resource_id is required")
        return rid

    def _request(self, method, url, *, params=None, json_body=None, files=None, stream=False, data_factory=None):
        headers = {"Authorization": f"Bearer {self.token_source.token()}"}
        if self.debug:
            parts = ["curl", "-s", "-X", method, "-H", "Authorization: Bearer <redacted>"]
            if json_body is not None:
                parts += ["-H", "Content-Type: application/json", "-d", json.dumps(json_body)[:2000]]
            parts.append(f"{url}?{urlencode(params)}" if params else url)
            log.debug(shlex.join(parts))
        resp = request_with_retry(self.session, method, url, retries=self.retries, params=params, json=json_body,
                                  files=files, stream=stream, headers=headers, data_factory=data_factory)
        if self.debug:
            log.debug("-> HTTP %s", resp.status_code)
        return resp

    def _check(self, resp, what):
        if resp.ok:
            return
        try:
            detail = resp.json()
        except ValueError:
            detail = resp.text[:500]
        if _is_sfapi(detail):  # often HTTP 403/404, but not a token problem
            raise SfapiUnavailable(f"IRI {what}: HTTP {resp.status_code}: {detail}", status=resp.status_code)
        if resp.status_code in (401, 403):
            raise LoginRequired(f"IRI {what}: HTTP {resp.status_code}: {detail}")
        raise RemoteError(f"IRI {what}: HTTP {resp.status_code}: {detail}", status=resp.status_code)

    def _json(self, resp, what):
        self._check(resp, what)
        if not resp.content:
            return {}
        try:
            result = resp.json()
        except ValueError:
            raise RemoteError(f"IRI {what}: non-JSON response: {resp.text[:200]}") from None
        if isinstance(result, dict) and "task_id" in result and "task_uri" in result:
            result = self._wait_for_task(result["task_id"], result["task_uri"], what)
        return result

    def _wait_for_task(self, task_id, task_uri, what):
        for attempt in range(1, self.task_max_polls + 1):
            task = self._json(self._request("GET", task_uri), f"{what} task {task_id}")
            status = task.get("status", "")
            if status in _TASK_TERMINAL_STATES:
                if status != "completed":
                    if _is_sfapi(task.get("result")):
                        raise SfapiUnavailable(f"IRI {what}: task {task_id} ended {status}: {task.get('result')}")
                    raise RemoteError(f"IRI {what}: task {task_id} ended {status}: {task.get('result')}")
                return task
            if attempt < self.task_max_polls:
                time.sleep(self.task_poll_interval)
        raise RemoteError(f"IRI {what}: task {task_id} not finished after {self.task_max_polls} polls")

    def _call(self, what, send, *, retry_errors=True):
        """send() (one request, and its task), sent again while it fails with SfapiUnavailable
        (sfapi_retries/sfapi_retry_sleep) or, if retry_errors, another retryable RemoteError
        (retry_attempts/retry_sleep). The two are counted separately."""
        sfapi_n = other_n = 0
        while True:
            try:
                return send()
            except SfapiUnavailable as exc:
                sfapi_n += 1
                if sfapi_n >= self.sfapi_retries:
                    raise SfapiUnavailable(f"{exc} (failed {sfapi_n} times in a row)", status=exc.status) from None
                log.warning("IRI %s: '%s' (attempt %d/%d); retrying in %ss",
                            what, SFAPI_ERROR, sfapi_n, self.sfapi_retries, self.sfapi_retry_sleep)
                time.sleep(self.sfapi_retry_sleep)
            except RemoteError as exc:
                if not retry_errors or not _retryable(exc):
                    raise
                other_n += 1
                if other_n >= self.retry_attempts:
                    if self.retry_attempts > 1:
                        raise RemoteError(f"{exc} (failed {other_n} times)", status=exc.status) from None
                    raise
                log.warning("IRI %s failed (attempt %d/%d): %s; retrying in %ss", what, other_n,
                            self.retry_attempts, str(exc).splitlines()[0][:200], self.retry_sleep)
                time.sleep(self.retry_sleep)

    def _api(self, method, url, what, *, retry_errors=True, **kw):
        return self._call(what, lambda: self._json(self._request(method, url, **kw), what),
                          retry_errors=retry_errors)

    def get_url(self, url, what="GET"):
        """GET an arbitrary IRI URL (used for token validation). Returns the response."""
        return self._request("GET", url)

    # ------------------------------------------------------------------ compute
    def launch_job(self, job_spec, *, resource_id=None):
        url = f"{self.base_url}/api/v1/compute/job/{_encode(self._rid(resource_id))}"
        # sfapi errors only: another error may hide a job that was accepted
        return self._api("POST", url, "submit job", json_body=job_spec, retry_errors=False)

    def get_job(self, job_id, *, resource_id=None):
        url = f"{self.base_url}/api/v1/compute/status/{_encode(self._rid(resource_id))}/{_encode(job_id)}"
        return self._api("GET", url, f"status of job {job_id}")

    def get_jobs(self, *, resource_id=None, offset=None, limit=None, historical=False, include_spec=False):
        url = f"{self.base_url}/api/v1/compute/status/{_encode(self._rid(resource_id))}"
        params = {}
        if offset is not None:
            params["offset"] = str(offset)
        if limit is not None:
            params["limit"] = str(limit)
        if historical:
            params["historical"] = "true"
        if include_spec:
            params["include_spec"] = "true"
        return self._api("POST", url, "list jobs", params=params, json_body={})

    def cancel_job(self, job_id, *, resource_id=None):
        url = f"{self.base_url}/api/v1/compute/cancel/{_encode(self._rid(resource_id))}/{_encode(job_id)}"
        return self._api("DELETE", url, f"cancel job {job_id}")

    # ------------------------------------------------------------------ filesystem
    def stat(self, path, *, resource_id=None, dereference=False):
        params = {"path": path}
        if dereference:
            params["dereference"] = "true"
        url = f"{self.base_url}/api/v1/filesystem/stat/{_encode(self._rid(resource_id))}"
        return self._api("GET", url, f"stat {path}", params=params)

    def ls(self, path, *, resource_id=None, show_hidden=False):
        params = {"path": path}
        if show_hidden:
            params["showHidden"] = "true"
        url = f"{self.base_url}/api/v1/filesystem/ls/{_encode(self._rid(resource_id))}"
        return self._api("GET", url, f"ls {path}", params=params)

    def mkdir(self, path, *, resource_id=None, parents=False):
        url = f"{self.base_url}/api/v1/filesystem/mkdir/{_encode(self._rid(resource_id))}"
        return self._api("POST", url, f"mkdir {path}", json_body={"path": path, "parent": parents})

    def chmod(self, path, mode, *, resource_id=None):
        url = f"{self.base_url}/api/v1/filesystem/chmod/{_encode(self._rid(resource_id))}"
        return self._api("PUT", url, f"chmod {path}", json_body={"path": path, "mode": str(mode)})

    def rm(self, path, *, resource_id=None):
        """Delete a file or directory (IRI `DELETE filesystem/rm`; runs as an async task)."""
        url = f"{self.base_url}/api/v1/filesystem/rm/{_encode(self._rid(resource_id))}"
        return self._api("DELETE", url, f"rm {path}", params={"path": path})

    def upload(self, local_path, remote_path, *, resource_id=None):
        url = f"{self.base_url}/api/v1/filesystem/upload/{_encode(self._rid(resource_id))}"
        what = f"upload {remote_path}"

        def send():
            with open(local_path, "rb") as fh:  # reopened for each attempt
                resp = self._request("POST", url, params={"path": remote_path}, files={"file": fh})
            return self._json(resp, what)

        return self._call(what, send)

    def download(self, remote_path, local_dest, *, resource_id=None):
        url = f"{self.base_url}/api/v1/filesystem/download/{_encode(self._rid(resource_id))}"
        self._call(f"download {remote_path}", lambda: self._download(url, remote_path, local_dest))

    def _download(self, url, remote_path, local_dest):
        resp = self._request("GET", url, params={"path": remote_path}, stream=True)
        self._check(resp, f"download {remote_path}")
        tmp = f"{local_dest}.part"
        if "application/json" in resp.headers.get("Content-Type", ""):
            data = resp.json()
            if "task_id" not in data or "task_uri" not in data:
                raise RemoteError(f"IRI download {remote_path}: unexpected response {data}")
            task = self._wait_for_task(data["task_id"], data["task_uri"], f"download {remote_path}")
            output = (task.get("result") or {}).get("output")
            if output is None:
                raise RemoteError(f"IRI download {remote_path}: task result has no output")
            with open(tmp, "w" if isinstance(output, str) else "wb") as fh:
                fh.write(output)
        else:
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    fh.write(chunk)
        os.replace(tmp, local_dest)
