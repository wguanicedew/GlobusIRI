"""IRI API client (ported from harvester iri_utils.IriClient).

Differences from the Harvester version: the access token comes from a TokenSource
(re-read when the token file changes), requests retry on connection errors and HTTP
429/5xx, and debug output never contains the bearer token.
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


def _encode(segment):
    return quote(str(segment), safe="")


class IriClient:
    def __init__(self, base_url, token_source, *, resource_id=None, retries=3, debug=False,
                 task_poll_interval=5, task_max_polls=60):
        self.base_url = base_url.rstrip("/")
        self.token_source = token_source
        self.resource_id = resource_id
        self.retries = retries
        self.debug = debug
        self.task_poll_interval = task_poll_interval
        self.task_max_polls = task_max_polls
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
                    raise RemoteError(f"IRI {what}: task {task_id} ended {status}: {task.get('result')}")
                return task
            if attempt < self.task_max_polls:
                time.sleep(self.task_poll_interval)
        raise RemoteError(f"IRI {what}: task {task_id} not finished after {self.task_max_polls} polls")

    def get_url(self, url, what="GET"):
        """GET an arbitrary IRI URL (used for token validation). Returns the response."""
        return self._request("GET", url)

    # ------------------------------------------------------------------ compute
    def launch_job(self, job_spec, *, resource_id=None):
        url = f"{self.base_url}/api/v1/compute/job/{_encode(self._rid(resource_id))}"
        return self._json(self._request("POST", url, json_body=job_spec), "submit job")

    def get_job(self, job_id, *, resource_id=None):
        url = f"{self.base_url}/api/v1/compute/status/{_encode(self._rid(resource_id))}/{_encode(job_id)}"
        return self._json(self._request("GET", url), f"status of job {job_id}")

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
        return self._json(self._request("POST", url, params=params, json_body={}), "list jobs")

    def cancel_job(self, job_id, *, resource_id=None):
        url = f"{self.base_url}/api/v1/compute/cancel/{_encode(self._rid(resource_id))}/{_encode(job_id)}"
        return self._json(self._request("DELETE", url), f"cancel job {job_id}")

    # ------------------------------------------------------------------ filesystem
    def stat(self, path, *, resource_id=None, dereference=False):
        params = {"path": path}
        if dereference:
            params["dereference"] = "true"
        url = f"{self.base_url}/api/v1/filesystem/stat/{_encode(self._rid(resource_id))}"
        return self._json(self._request("GET", url, params=params), f"stat {path}")

    def ls(self, path, *, resource_id=None, show_hidden=False):
        params = {"path": path}
        if show_hidden:
            params["showHidden"] = "true"
        url = f"{self.base_url}/api/v1/filesystem/ls/{_encode(self._rid(resource_id))}"
        return self._json(self._request("GET", url, params=params), f"ls {path}")

    def mkdir(self, path, *, resource_id=None, parents=False):
        url = f"{self.base_url}/api/v1/filesystem/mkdir/{_encode(self._rid(resource_id))}"
        return self._json(self._request("POST", url, json_body={"path": path, "parent": parents}), f"mkdir {path}")

    def chmod(self, path, mode, *, resource_id=None):
        url = f"{self.base_url}/api/v1/filesystem/chmod/{_encode(self._rid(resource_id))}"
        return self._json(self._request("PUT", url, json_body={"path": path, "mode": str(mode)}), f"chmod {path}")

    def rm(self, path, *, resource_id=None):
        """Delete a file or directory (IRI `DELETE filesystem/rm`; runs as an async task)."""
        url = f"{self.base_url}/api/v1/filesystem/rm/{_encode(self._rid(resource_id))}"
        return self._json(self._request("DELETE", url, params={"path": path}), f"rm {path}")

    def upload(self, local_path, remote_path, *, resource_id=None):
        url = f"{self.base_url}/api/v1/filesystem/upload/{_encode(self._rid(resource_id))}"
        with open(local_path, "rb") as fh:
            resp = self._request("POST", url, params={"path": remote_path}, files={"file": fh})
        return self._json(resp, f"upload {remote_path}")

    def download(self, remote_path, local_dest, *, resource_id=None):
        url = f"{self.base_url}/api/v1/filesystem/download/{_encode(self._rid(resource_id))}"
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
