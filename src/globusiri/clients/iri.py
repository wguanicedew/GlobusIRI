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
  directory" (a missing path; cleanup relies on it);
* rejected requests (RequestRejected): HTTP 400, 405, 422 and 501. The NERSC and ALCF OpenAPI
  specs answer every error with an RFC 9457 Problem; these codes mean the request itself is
  wrong or unsupported (for example sbatch refusing a job: "More resources requested than
  allowed for logical queue ...", "your balance is 0.00 node hours"), so the workflow fails
  the run instead of retrying. For job submission, 404 (unknown resource) and 409 (a
  conflicting submission) are rejections too;
* job submission, where a lost reply may hide an accepted job: retrying could start the job
  twice (the workflow's submit step has its own attempts, and a job is never duplicated
  by the client);
* 401/403 token problems (LoginRequired).

API versions (`api_version`, site setting `iri.api_version`):

* v1 (NERSC, https://api.iri.nersc.gov/openapi.json): `/api/v1`; filesystem stat, ls and download
  are GET and rm is DELETE, each with query parameters; chmod is PUT.
* v2 (ALCF, https://api.alcf.anl.gov/api/v2/openapi.json): `/api/v2`; stat, ls, rm, download
  and chmod are POST with a JSON body.

Compute (job, status, cancel), mkdir, upload and task calls are the same in both, apart from
the prefix.
"""

import json
import logging
import os
import shlex
import time
from urllib.parse import quote, urlencode

import requests

from ..errors import ConfigError, LoginRequired, RemoteError, RequestRejected
from .http import request_with_retry

log = logging.getLogger("globusiri.iri")

_TASK_TERMINAL_STATES = {"completed", "failed", "canceled"}
API_VERSIONS = ("v1", "v2")
SFAPI_ERROR = "Unable to get sfapi data"
# HTTP statuses that reject the request itself (see the module docstring); 401/403 are token
# problems (LoginRequired), 429/5xx other than 501 are transient
REJECTED_STATUS = (400, 405, 422, 501)
# extra statuses that reject a job submission: unknown resource, conflicting submission
SUBMIT_REJECTED_STATUS = (404, 409)


class SfapiUnavailable(RemoteError):
    """NERSC's transient "Unable to get sfapi data" error (raised after the retries)."""


def _is_sfapi(detail):
    return SFAPI_ERROR in str(detail)


def _retryable(exc):
    """A RemoteError worth another attempt (see the module docstring)."""
    return (not isinstance(exc, RequestRejected) and exc.status != 404
            and "No such file or directory" not in str(exc))


def _encode(segment):
    return quote(str(segment), safe="")


class IriClient:
    def __init__(self, base_url, token_source, *, resource_id=None, api_version="v1", retries=3, debug=False,
                 task_poll_interval=5, task_max_polls=60, sfapi_retries=3, sfapi_retry_sleep=3,
                 retry_attempts=3, retry_sleep=3):
        self.base_url = base_url.rstrip("/")
        if api_version not in API_VERSIONS:
            raise ConfigError(f"IRI api_version must be one of {API_VERSIONS}, not '{api_version}'")
        self.api_version = api_version
        self.api_url = f"{self.base_url}/api/{api_version}"
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
    @property
    def v2(self):
        return self.api_version == "v2"

    def _fs_url(self, op, resource_id):
        return f"{self.api_url}/filesystem/{op}/{_encode(self._rid(resource_id))}"

    def _fs_read(self, op, method_v1, what, args, resource_id, **kw):
        """A filesystem call that v1 sends as method_v1 with query parameters and v2 as POST
        with a JSON body (args: v2 JSON values; v1 gets them as strings, booleans lowercased)."""
        url = self._fs_url(op, resource_id)
        if self.v2:
            return self._api("POST", url, what, json_body=args, **kw)
        params = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in args.items()}
        return self._api(method_v1, url, what, params=params, **kw)

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
        if resp.status_code in REJECTED_STATUS:
            raise RequestRejected(f"IRI {what}: HTTP {resp.status_code}: {detail}", status=resp.status_code)
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

    def get_url(self, url, what="GET", *, method="GET", json_body=None):
        """Send one request to an arbitrary IRI URL (used for token validation). Returns the response."""
        return self._request(method, url, json_body=json_body)

    # ------------------------------------------------------------------ compute
    def launch_job(self, job_spec, *, resource_id=None):
        url = f"{self.api_url}/compute/job/{_encode(self._rid(resource_id))}"
        # sfapi errors only: another error may hide a job that was accepted
        try:
            return self._api("POST", url, "submit job", json_body=job_spec, retry_errors=False)
        except (SfapiUnavailable, RequestRejected):
            raise
        except RemoteError as exc:
            if exc.status in SUBMIT_REJECTED_STATUS:
                raise RequestRejected(str(exc), status=exc.status) from None
            raise

    def get_job(self, job_id, *, resource_id=None):
        url = f"{self.api_url}/compute/status/{_encode(self._rid(resource_id))}/{_encode(job_id)}"
        return self._api("GET", url, f"status of job {job_id}")

    def get_jobs(self, *, resource_id=None, offset=None, limit=None, historical=False, include_spec=False):
        url = f"{self.api_url}/compute/status/{_encode(self._rid(resource_id))}"
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
        url = f"{self.api_url}/compute/cancel/{_encode(self._rid(resource_id))}/{_encode(job_id)}"
        return self._api("DELETE", url, f"cancel job {job_id}")

    # ------------------------------------------------------------------ filesystem
    def stat(self, path, *, resource_id=None, dereference=False):
        args = {"path": path}
        if dereference:
            args["dereference"] = True
        return self._fs_read("stat", "GET", f"stat {path}", args, resource_id)

    def ls(self, path, *, resource_id=None, show_hidden=False):
        args = {"path": path}
        if show_hidden:
            args["showHidden"] = True
        return self._fs_read("ls", "GET", f"ls {path}", args, resource_id)

    def mkdir(self, path, *, resource_id=None, parents=False):
        url = self._fs_url("mkdir", resource_id)
        return self._api("POST", url, f"mkdir {path}", json_body={"path": path, "parent": parents})

    def chmod(self, path, mode, *, resource_id=None):
        url = self._fs_url("chmod", resource_id)
        return self._api("POST" if self.v2 else "PUT", url, f"chmod {path}",
                         json_body={"path": path, "mode": str(mode)})

    def rm(self, path, *, resource_id=None):
        """Delete a file or directory (v1 `DELETE filesystem/rm`, v2 `POST`; runs as an async task)."""
        return self._fs_read("rm", "DELETE", f"rm {path}", {"path": path}, resource_id)

    def upload(self, local_path, remote_path, *, resource_id=None):
        url = self._fs_url("upload", resource_id)
        what = f"upload {remote_path}"

        def send():
            with open(local_path, "rb") as fh:  # reopened for each attempt
                resp = self._request("POST", url, params={"path": remote_path}, files={"file": fh})
            return self._json(resp, what)

        return self._call(what, send)

    def download(self, remote_path, local_dest, *, resource_id=None):
        url = self._fs_url("download", resource_id)
        self._call(f"download {remote_path}", lambda: self._download(url, remote_path, local_dest))

    def _download(self, url, remote_path, local_dest):
        if self.v2:
            resp = self._request("POST", url, json_body={"path": remote_path}, stream=True)
        else:
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
