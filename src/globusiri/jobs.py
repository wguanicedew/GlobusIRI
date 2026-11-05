"""IRI jobs: render the batch script, prepare the remote run dir, submit, monitor, cancel.

The job_spec layout and the template placeholder names follow harvester's
IriSubmitter, so existing templates (perlmutter/crux_iri_submit_template.sh) work as-is.
"""

import json
import logging
import os
import posixpath
import secrets
import stat
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from math import ceil

from .errors import JobFailed, RemoteError

log = logging.getLogger("globusiri.jobs")

EXECUTABLE = "executable_batch"

# IRI status.state -> GlobusIRI state
SUBMITTED, RUNNING, FINISHED, FAILED, CANCELLED = "submitted", "running", "finished", "failed", "cancelled"
TERMINAL = (FINISHED, FAILED, CANCELLED)


class SafeDict(dict):
    """format_map helper: unknown {keys} are left untouched (so ${bash_var} survives)."""

    def __missing__(self, key):
        return "{" + key + "}"


def new_run_id():
    return f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(3)}"


def map_state(job):
    status = job.get("status") or {}
    state = (status.get("state") or "").lower()
    exit_code = status.get("exit_code")
    if state in ("new", "queued"):
        return SUBMITTED
    if state == "active":
        return RUNNING
    if state == "completed":
        return FINISHED if exit_code in (None, 0) else FAILED
    if state == "canceled":
        return CANCELLED
    return FAILED


def stdio_paths_from_job(job):
    """(stdout, stderr) from the scheduler's admincomment, if the IRI job report has it."""
    meta = (job.get("status") or {}).get("meta_data") or {}
    comment = meta.get("admincomment")
    if not comment:
        return None, None
    try:
        comment = json.loads(comment) if isinstance(comment, str) else comment
    except (TypeError, ValueError):
        return None, None
    if not isinstance(comment, dict):
        return None, None
    return comment.get("stdoutPath"), comment.get("stderrPath")


def _int_or_float(x):
    return int(x) if float(x).is_integer() else x


def placeholders(job, site, run_id, local_inputs):
    """Template placeholders, using harvester's names."""
    r = job.resources
    n_node = int(r.get("n_node", 1))
    n_core_per_node = int(r.get("n_core_per_node", 1))
    n_process_per_node = int(r.get("n_process_per_node", 1))
    walltime = job.duration or 0
    remote_dir = site.remote_run_dir(run_id)
    remote_log = site.remote_log_dir(run_id)
    ph = {
        "nNode": n_node,
        "nCorePerNode": n_core_per_node,
        "nProcessPerNode": n_process_per_node,
        "nCorePerProcess": _int_or_float(n_core_per_node / n_process_per_node),
        "nCoreTotal": n_core_per_node * n_node,
        "nCoreFactor": 1,
        "requestWalltime": walltime,
        "requestWalltimeMinute": ceil(walltime / 60),
        "requestCputime": walltime * n_core_per_node * n_node,
        "requestCputimeMinute": ceil(walltime * n_core_per_node * n_node / 60),
        "harvesterID": "globusiri",
        "workerID": run_id,
        "runID": run_id,
        "computingSite": site.name,
        "pandaQueueName": site.name,
        "localQueueName": r.get("queue") or "",
        "gtag": "unknown",
        "jobType": "",
        "prodSourceLabel": "",
        "pilotType": "",
        "work_dir": remote_dir,
        "remote_log_dir": remote_log,
        "stdout_path": posixpath.join(remote_log, f"{run_id}_stdout.txt"),
        "stderr_path": posixpath.join(remote_log, f"{run_id}_stderr.txt"),
        "tokenOrigin": job.panda_auth_origin,
        "project": r.get("project") or "",
    }
    # each input placeholder is its remote file name if the file is uploaded, else ""
    for name in job.inputs:
        ph[name] = name if name in local_inputs else ""
    ph.update(job.template_vars)
    return ph


def render_template(template_path, ph, out_path):
    text = template_path.read_text(encoding="utf-8")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text.format_map(SafeDict(ph)), encoding="utf-8")
    os.chmod(out_path, os.stat(out_path).st_mode | stat.S_IXUSR | stat.S_IRGRP | stat.S_IROTH)
    return out_path


def _prune(d):
    return {k: v for k, v in d.items() if v is not None}


def build_job_spec(job, site, ph):
    r = job.resources
    n_node = ph["nNode"]
    resources = {
        "node_count": n_node,
        "process_count": n_node * ph["nProcessPerNode"],
        "processes_per_node": ph["nProcessPerNode"],
        "cpu_cores_per_process": ph["nCorePerProcess"],
        "exclusive_node_use": bool(r.get("exclusive_node_use", True)),
        "memory": int(r["memory"]) if r.get("memory") else None,
    }
    gpu = int(r.get("gpu_cores_per_process") or 0)
    if gpu >= 1:  # IRI rejects gpu_cores_per_process < 1
        resources["gpu_cores_per_process"] = gpu
    for f in site.unsupported_resource_fields:
        resources.pop(f, None)

    custom = {}
    for key in ("constraint", "signal", "licenses", "export", "image", "volume"):
        if r.get(key) is not None:
            custom[key] = r[key]
    if r.get("no_requeue"):
        custom["no_requeue"] = True
    custom.update(r.get("custom_attributes") or {})

    attributes = _prune({
        "duration": job.duration,
        "queue_name": r.get("queue"),
        "account": r.get("project"),
        "reservation_id": r.get("reservation_id"),
    })
    if custom:
        attributes["custom_attributes"] = custom

    spec = {
        "executable": posixpath.join(ph["work_dir"], EXECUTABLE),
        "arguments": [],
        "directory": ph["work_dir"],
        "name": f"{job.name}-{ph['runID']}",
        "inherit_environment": True,
        "stdout_path": ph["stdout_path"],
        "stderr_path": ph["stderr_path"],
        "resources": _prune(resources),
        "attributes": attributes,
        "pre_launch": r.get("pre_launch"),
        "post_launch": r.get("post_launch"),
        "launcher": r.get("launcher"),
    }
    return _prune(spec)


@dataclass
class Submission:
    run_id: str
    site: str
    remote_dir: str
    remote_log_dir: str
    stdout_path: str
    stderr_path: str
    script: str
    job_spec: dict
    uploaded: list = field(default_factory=list)
    skipped_inputs: list = field(default_factory=list)
    job_id: str = None

    def to_dict(self):
        return asdict(self)


def prepare(config, job, site, run_id, run_dir):
    """Snapshot the job, render the script and build job_spec (no remote calls)."""
    local_inputs, skipped = {}, []
    for name, path in job.inputs.items():
        p = config.paths.resolve(path) if path else None
        if p and p.is_file():
            local_inputs[name] = p
        else:
            skipped.append(f"{name} ({path})")
    job.snapshot(run_dir / "job.yaml")
    ph = placeholders(job, site, run_id, local_inputs)
    if not job.template_file:
        from .errors import ConfigError

        raise ConfigError("job has no template_file")
    script = render_template(config.paths.find_template(job.template_file), ph, run_dir / EXECUTABLE)
    spec = build_job_spec(job, site, ph)
    sub = Submission(run_id=run_id, site=site.name, remote_dir=ph["work_dir"], remote_log_dir=ph["remote_log_dir"],
                     stdout_path=ph["stdout_path"], stderr_path=ph["stderr_path"], script=str(script),
                     job_spec=spec, skipped_inputs=skipped)
    return sub, local_inputs


def upload_inputs(config, site, sub, local_inputs, iri, https=None):
    """mkdir the remote dirs, upload executable_batch + inputs, chmod the executable."""
    iri.mkdir(sub.remote_dir, resource_id=site.work_dir_resource_id, parents=True)
    if sub.remote_log_dir != sub.remote_dir:
        iri.mkdir(sub.remote_log_dir, resource_id=site.log_dir_resource_id, parents=True)
    files = {EXECUTABLE: sub.script, **{k: str(v) for k, v in local_inputs.items()}}
    for name, local in files.items():
        remote = posixpath.join(sub.remote_dir, name)
        if site.upload_method == "globus_https":
            https.put(local, remote)
        else:
            iri.upload(local, remote, resource_id=site.work_dir_resource_id)
        sub.uploaded.append(name)
    iri.chmod(posixpath.join(sub.remote_dir, EXECUTABLE), "0755", resource_id=site.work_dir_resource_id)


def launch(site, sub, iri):
    job = iri.launch_job(sub.job_spec, resource_id=site.resource_id)
    job_id = job.get("id")
    if not job_id:
        raise RemoteError(f"IRI submission returned no job id: {job}")
    sub.job_id = str(job_id)
    return sub.job_id


def status(iri, site, job_id):
    job = iri.get_job(job_id, resource_id=site.resource_id)
    return map_state(job), job


def wait(iri, site, job_id, *, interval=60, timeout=None, on_update=None):
    """Poll until terminal. Returns (state, job)."""
    start = time.time()
    last = None
    while True:
        state, job = status(iri, site, job_id)
        if state != last and on_update:
            on_update(state, job)
        last = state
        if state in TERMINAL:
            return state, job
        if timeout and time.time() - start > timeout:
            raise JobFailed(f"timed out after {timeout}s waiting for job {job_id} (state {state})")
        time.sleep(interval)


def cancel(iri, site, job_id):
    return iri.cancel_job(job_id, resource_id=site.resource_id)
