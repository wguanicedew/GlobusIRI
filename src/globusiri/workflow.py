"""End-to-end run: validate -> [e2e stagein] -> upload -> submit -> monitor ->
https download of stdout/stderr -> [e2e stageout] -> [cleanup].

Progress is kept in <WORK_DIR>/runs/<run_id>/state.json next to job.yaml (the resolved
job snapshot). `--resume` skips steps already marked done.
"""

import json
import logging
from datetime import datetime, timezone

import yaml

from . import cleanup as cleanup_mod
from . import jobs, services
from .errors import ConfigError, JobFailed, ValidationFailed
from .jobfile import resolve_job
from .secure_io import write_private
from .staging import e2e as e2e_staging
from .staging import https as https_staging
from .validate import FAIL, failed, validate

log = logging.getLogger("globusiri.workflow")

STEPS = ("validate", "e2e_stagein", "upload", "submit", "monitor", "https_stageout", "e2e_stageout", "cleanup")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class RunState:
    def __init__(self, run_dir, data=None):
        self.run_dir = run_dir
        self.path = run_dir / "state.json"
        self.data = data or {"steps": {s: {"status": "pending"} for s in STEPS}}

    @classmethod
    def load(cls, run_dir):
        path = run_dir / "state.json"
        if not path.is_file():
            raise ConfigError(f"no run state at {path}")
        return cls(run_dir, json.loads(path.read_text()))

    def save(self):
        self.run_dir.mkdir(parents=True, exist_ok=True)
        write_private(self.path, json.dumps(self.data, indent=2) + "\n")

    def step(self, name):
        return self.data["steps"].setdefault(name, {"status": "pending"})

    def done(self, name):
        return self.step(name).get("status") in ("done", "skipped")

    def set(self, name, status, **extra):
        st = self.step(name)
        st.update(extra)
        st["status"] = status
        st["updated_at"] = _now()
        self.save()


def _apply_manifest_overrides(job, stagein_manifest, stageout_manifest):
    if not (stagein_manifest or stageout_manifest):
        return
    e2e = job.data.setdefault("staging", {}).setdefault("e2e", {})
    e2e["enabled"] = True
    if stagein_manifest:
        e2e.setdefault("stagein", {})["manifest"] = str(stagein_manifest)
    if stageout_manifest:
        e2e.setdefault("stageout", {})["manifest"] = str(stageout_manifest)


def _has_files(spec):
    return bool(spec and (spec.get("manifest") or spec.get("rucio")))


def run(config, job, site, *, run_id=None, no_wait=False, do_validate=True, stagein_manifest=None,
        stageout_manifest=None, interval=60, echo=print, resume_state=None):
    """Run (or resume) the workflow. Returns the RunState."""
    if resume_state is None:
        _apply_manifest_overrides(job, stagein_manifest, stageout_manifest)
        run_id = run_id or jobs.new_run_id()
        run_dir = config.paths.run_dir(run_id)
        if (run_dir / "state.json").exists():
            raise ConfigError(f"run {run_id} already exists; use --resume {run_id}")
        state = RunState(run_dir)
        state.data.update({"run_id": run_id, "site": site.name, "created_at": _now(),
                           "remote_work_dir": site.remote_run_dir(run_id)})
        job.snapshot(run_dir / "job.yaml")
        state.save()
    else:
        state = resume_state
        run_id = state.data["run_id"]
    run_dir = state.run_dir
    echo(f"run {run_id} at {site.name} ({run_dir})")

    # 1. validate the tokens this job needs
    if do_validate:
        checks = validate(config, job=job)
        for c in checks:
            if c.status != "OK":
                echo(f"  validate {c.kind:5} {c.check:14} {c.status:7} {c.detail_with_fix}")
        if failed(checks):
            state.set("validate", "failed", failures=[c.to_dict() for c in checks if c.status == FAIL])
            raise ValidationFailed("token validation failed; nothing was transferred or submitted")
        state.set("validate", "done")
    else:
        state.set("validate", "skipped", reason="--no-validate")

    transfer = services.transfer(config) if job.e2e_enabled else None

    # 2. e2e stagein
    if not state.done("e2e_stagein"):
        spec = job.e2e.get("stagein") or {}
        if job.e2e_enabled and _has_files(spec):
            task_ids = state.step("e2e_stagein").get("task_ids")
            if not task_ids:
                p = e2e_staging.plan(config, site, "stagein", manifest=config.paths.resolve(spec.get("manifest")) if spec.get("manifest") else None,
                                     rucio=spec.get("rucio") or [])
                task_ids = e2e_staging.submit(transfer, config, p, label=f"globusiri-{run_id}-IN")
                state.set("e2e_stagein", "running", task_ids=task_ids, files=len(p.items))
            echo(f"  e2e stagein: waiting for {', '.join(task_ids)}")
            e2e_staging.wait(transfer, task_ids, poll_interval=config.e2e.poll_interval, timeout=config.e2e.timeout)
            state.set("e2e_stagein", "done")
        else:
            state.set("e2e_stagein", "skipped", reason="job does not enable e2e stagein")

    # 3. upload (render + mkdir + upload + chmod)
    iri = services.iri_client(config, site)
    https = services.https_client(config, site) if site.has_https and (site.upload_method == "globus_https" or job.wants_https_download()) else None
    sub_data = state.step("upload").get("submission")
    if not state.done("upload"):
        sub, local_inputs = jobs.prepare(config, job, site, run_id, run_dir)
        for s in sub.skipped_inputs:
            echo(f"  input skipped (not found): {s}")
        jobs.upload_inputs(config, site, sub, local_inputs, iri, https)
        sub_data = sub.to_dict()
        state.set("upload", "done", submission=sub_data, method=site.upload_method)
        echo(f"  uploaded {', '.join(sub.uploaded)} to {sub.remote_dir} via {site.upload_method}")
    sub = jobs.Submission(**sub_data)

    # 4. submit
    if not state.done("submit"):
        job_id = jobs.launch(site, sub, iri)
        state.set("submit", "done", job_id=job_id)
        echo(f"  submitted IRI job {job_id}")
    job_id = state.step("submit")["job_id"]
    if no_wait:
        echo(f"  not waiting; resume with: globusiri run --resume {run_id}")
        return state

    # 5. monitor
    report = None
    if not state.done("monitor"):
        def on_update(st, rep):
            state.set("monitor", "running", state=st)
            echo(f"  job {job_id}: {st}")

        final, report = jobs.wait(iri, site, job_id, interval=interval, on_update=on_update)
        state.set("monitor", "done", state=final,
                  exit_code=(report.get("status") or {}).get("exit_code"))
    final = state.step("monitor")["state"]

    # 6. https download of stdout/stderr (+ extra files)
    if not state.done("https_stageout"):
        if job.wants_https_download() and https is not None:
            if report is None:
                _, report = jobs.status(iri, site, job_id)
            h = job.https_stageout
            files = h.get("files") or []
            if h.get("stdout_stderr"):
                res = https_staging.stageout_job_logs(https, report, site, run_dir, run_id=run_id, extra_files=files)
            else:
                import posixpath

                res = https_staging.stageout(https, [posixpath.join(sub.remote_dir, f) for f in files], run_dir)
            errors = {k: v for k, v in res.items() if str(v).startswith("error")}
            state.set("https_stageout", "done" if not errors else "failed", files=res)
            for remote, local in res.items():
                echo(f"  download {remote} -> {local}")
        else:
            state.set("https_stageout", "skipped", reason="job does not download over HTTPS")

    # 7. e2e stageout (only after a successful job)
    if not state.done("e2e_stageout"):
        spec = job.e2e.get("stageout") or {}
        if job.e2e_enabled and _has_files(spec) and final == jobs.FINISHED:
            task_ids = state.step("e2e_stageout").get("task_ids")
            if not task_ids:
                p = e2e_staging.plan(config, site, "stageout", manifest=config.paths.resolve(spec.get("manifest")) if spec.get("manifest") else None,
                                     rucio=spec.get("rucio") or [])
                task_ids = e2e_staging.submit(transfer, config, p, label=f"globusiri-{run_id}-OUT")
                state.set("e2e_stageout", "running", task_ids=task_ids, files=len(p.items))
            echo(f"  e2e stageout: waiting for {', '.join(task_ids)}")
            e2e_staging.wait(transfer, task_ids, poll_interval=config.e2e.poll_interval, timeout=config.e2e.timeout)
            state.set("e2e_stageout", "done")
        elif job.e2e_enabled and _has_files(spec):
            state.set("e2e_stageout", "skipped", reason=f"job ended {final}")
        else:
            state.set("e2e_stageout", "skipped", reason="job does not enable e2e stageout")

    # 8. cleanup on the HPC (opt-in, only after a fully successful run)
    if not state.done("cleanup"):
        _cleanup(config, job, site, state, iri, transfer, final, echo)

    if final != jobs.FINISHED:
        raise JobFailed(f"IRI job {job_id} ended {final}")
    echo(f"run {run_id} finished")
    return state


def _cleanup(config, job, site, state, iri, transfer, final, echo):
    want = job.cleanup
    run_id = state.data["run_id"]
    in_spec = job.e2e.get("stagein") or {}
    do_e2e = bool(want.get("e2e_stagein")) and job.e2e_enabled and _has_files(in_spec)
    do_run = bool(want.get("remote_run_dir"))
    if not (do_e2e or do_run):
        state.set("cleanup", "skipped", reason="job does not enable cleanup")
        return
    if final != jobs.FINISHED:
        state.set("cleanup", "skipped", reason=f"job ended {final}; files kept for debugging")
        return
    bad = [s for s in ("https_stageout", "e2e_stageout") if state.step(s).get("status") not in ("done", "skipped")]
    if bad:
        state.set("cleanup", "skipped", reason=f"{', '.join(bad)} not done; files kept")
        return
    step = state.step("cleanup")
    if do_e2e:
        task_ids = step.get("e2e_task_ids")
        if not task_ids:
            manifest = config.paths.resolve(in_spec.get("manifest")) if in_spec.get("manifest") else None
            p = e2e_staging.cleanup_plan(site, manifest=manifest, rucio=in_spec.get("rucio") or [])
            task_ids = e2e_staging.submit_delete(transfer, config, p, label=f"globusiri-{run_id}-CLEANUP")
            state.set("cleanup", "running", e2e_task_ids=task_ids, e2e_files=len(p.items))
        echo(f"  cleanup e2e stage-in files: waiting for {', '.join(task_ids)}")
        e2e_staging.wait(transfer, task_ids, poll_interval=config.e2e.poll_interval, timeout=config.e2e.timeout)
    if do_run:
        removed = cleanup_mod.cleanup_run(iri, site, run_id)
        for path, what in removed.items():
            echo(f"  cleanup {path}: {what}")
        state.set("cleanup", "running", removed=removed)
    state.set("cleanup", "done")


def resume(config, run_id, **kwargs):
    run_dir = config.paths.run_dir(run_id)
    state = RunState.load(run_dir)
    snap = yaml.safe_load((run_dir / "job.yaml").read_text())
    job, site = resolve_job(config, snapshot_data=snap)
    return run(config, job, site, resume_state=state, **kwargs)
