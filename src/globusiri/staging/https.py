#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Globus HTTPS staging. stagein = upload to the HPC, stageout = download from it.

Remote paths are HPC mount paths; the client maps them through the site's https.path_map.
"""

import os
import posixpath
from pathlib import Path

from ..errors import GlobusIriError
from ..jobs import stdio_paths_from_job


def stagein(https, local_files, remote_dir, *, iri=None, site=None, mkdir=False, executable=None):
    """Upload local files into remote_dir. Returns the list of remote paths.

    HTTPS PUT does not reliably create parent directories, so with mkdir=True the target
    directory is created through IRI first. HTTPS does not keep file modes, so files named
    in `executable` are chmod 0755 through IRI afterwards.
    """
    if (mkdir or executable) and (iri is None or site is None):
        raise GlobusIriError("mkdir/executable need the IRI client")
    if mkdir:
        iri.mkdir(remote_dir, resource_id=site.work_dir_resource_id, parents=True)
    done = []
    for local in local_files:
        local = Path(local)
        if not local.is_file():
            raise GlobusIriError(f"local file {local} not found")
        remote = posixpath.join(remote_dir, local.name)
        https.put(local, remote)
        done.append(remote)
    for name in executable or []:
        iri.chmod(posixpath.join(remote_dir, name), "0755", resource_id=site.work_dir_resource_id)
    return done


def stageout(https, remote_files, local_dir, *, skip_existing=True):
    """Download remote files into local_dir. Returns {remote: local or 'skipped'}."""
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    result = {}
    for remote in remote_files:
        dest = local_dir / posixpath.basename(remote)
        if skip_existing and dest.exists():
            result[remote] = "skipped"
            continue
        https.get(remote, dest)
        os.chmod(dest, 0o644)
        result[remote] = str(dest)
    return result


def job_stdio_paths(job_report, site, run_id=None):
    """stdout/stderr paths: the scheduler's admincomment, else <log_dir>/<run>/<run>_std{out,err}.txt."""
    out, err = stdio_paths_from_job(job_report or {})
    if (not out or not err) and run_id:
        base = site.remote_log_dir(run_id)
        out = out or posixpath.join(base, f"{run_id}_stdout.txt")
        err = err or posixpath.join(base, f"{run_id}_stderr.txt")
    return [p for p in (out, err) if p]


def stageout_job_logs(https, job_report, site, local_dir, *, run_id=None, extra_files=(), skip_existing=True):
    """Download a job's stdout/stderr (+ extra files relative to the run dir)."""
    remote = job_stdio_paths(job_report, site, run_id)
    if run_id:
        remote += [posixpath.join(site.remote_run_dir(run_id), f) for f in extra_files]
    if not remote:
        raise GlobusIriError("cannot determine stdout/stderr paths: the job report has no admincomment; pass --run-id")
    results = {}
    for path in remote:
        try:
            results.update(stageout(https, [path], local_dir, skip_existing=skip_existing))
        except GlobusIriError as exc:
            results[path] = f"error: {exc}"
    return results
