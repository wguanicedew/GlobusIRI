#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Remove files that GlobusIRI left on the HPC.

* IRI: remote run directories (`<remote.work_dir>/<run_id>`, `<remote.log_dir>/<run_id>`)
  are removed with the IRI `filesystem/rm` call.
* E2E: files staged in with Globus Transfer are removed with a Globus delete task
  (see staging/e2e.py `cleanup_plan`).

Every path is checked against the site's own directories first, so a typo cannot remove
something outside the GlobusIRI area unless `force` is given.
"""

import logging
import posixpath

from .errors import ConfigError, RemoteError

log = logging.getLogger("globusiri.cleanup")


def site_roots(site):
    """HPC directories (mount paths) that GlobusIRI may clean up at this site."""
    roots = [site.work_dir, site.log_dir, getattr(site, "e2e_base_path", None)]
    out = []
    for r in roots:
        if r:
            r = posixpath.normpath(r)
            if r not in out:
                out.append(r)
    return out


def check_under(path, roots, *, force=False, what="path"):
    """Return the normalized path; refuse it unless it is strictly below one of roots."""
    if not path or not path.startswith("/"):
        raise ConfigError(f"{what} must be an absolute HPC path, got '{path}'")
    norm = posixpath.normpath(path)
    if norm == "/":
        raise ConfigError("refusing to remove '/'")
    if force:
        return norm
    for root in roots:
        if norm != root and norm.startswith(root.rstrip("/") + "/"):
            return norm
    raise ConfigError(f"{what} {norm} is not below any of {', '.join(roots) or '(none)'}; "
                      "pass --force to remove it anyway")


def iri_rm(iri, path, *, resource_id=None, missing_ok=True):
    """Remove one path with IRI. Returns True if removed, False if it was already gone."""
    try:
        iri.rm(path, resource_id=resource_id)
    except RemoteError as exc:
        # facilities report a missing path differently (404, or a failed async task),
        # so confirm with stat before treating the error as "already gone"
        if missing_ok and (exc.status == 404 or _missing(iri, path, resource_id)):
            log.info("rm %s: already gone", path)
            return False
        raise
    log.info("removed %s", path)
    return True


def _missing(iri, path, resource_id):
    try:
        iri.stat(path, resource_id=resource_id)
    except RemoteError as exc:
        return exc.status == 404
    return False


def run_dirs(site, run_id):
    """[(path, resource_id)] of the remote directories of one run."""
    if not run_id or "/" in run_id or run_id in (".", ".."):
        raise ConfigError(f"invalid run id '{run_id}'")
    out = [(site.remote_run_dir(run_id), site.work_dir_resource_id)]
    log_dir = site.remote_log_dir(run_id)
    if log_dir != out[0][0]:
        out.append((log_dir, site.log_dir_resource_id))
    return out


def cleanup_run(iri, site, run_id):
    """Remove the remote run directory (and log directory, if separate) of a run.

    Returns {path: "removed" | "already gone"}.
    """
    roots = site_roots(site)
    result = {}
    for path, rid in run_dirs(site, run_id):
        path = check_under(path, roots, what="run directory")
        result[path] = "removed" if iri_rm(iri, path, resource_id=rid) else "already gone"
    return result
