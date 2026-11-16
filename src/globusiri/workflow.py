"""Workflow engine: validate -> [e2e stagein] -> upload -> submit -> monitor ->
https download of stdout/stderr -> [e2e stageout] -> [cleanup].

The engine never blocks. `advance()` makes one pass over a run. It goes through the steps
in order, starts the next one, or checks a step that is already running (a Globus task, an
IRI job), and stops when a step is still in progress. Calling `advance()` repeatedly drives
a run to the end:

* plugin mode: the caller (e.g. Harvester's monitor cycle) calls `monitor()` each cycle;
* daemon mode: `daemon()` calls `monitor()` in a loop;
* blocking mode: `run()` calls `advance()` until the run ends (the `globusiri run` command).

Progress is kept in <WORK_DIR>/runs/<run_id>/state.json next to job.yaml (the resolved
job snapshot). A per-run lock file makes concurrent callers skip a run that another
process is advancing, instead of doing the same step twice.
"""

import fcntl
import json
import logging
import os
import posixpath
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import yaml

from . import cleanup as cleanup_mod
from . import jobs, services
from .errors import ConfigError, JobFailed, ValidationFailed
from .jobfile import resolve_job
from .runindex import RunIndex
from .runlog import run_log_path, run_logging
from .staging import e2e as e2e_staging
from .staging import https as https_staging
from .validate import FAIL, failed, validate

log = logging.getLogger("globusiri.workflow")

STEPS = ("validate", "e2e_stagein", "upload", "submit", "monitor", "https_stageout", "e2e_stageout", "cleanup")

# run status
ACTIVE, FINISHED, FAILED, CANCELLED = "active", "finished", "failed", "cancelled"
RUN_TERMINAL = (FINISHED, FAILED, CANCELLED)

# step status that lets the run move on
STEP_PASSED = ("done", "skipped")

# errors that end a run at once; anything else (network, 5xx, token briefly missing) is
# retried on the next pass, up to max_attempts per step
FATAL = (ConfigError, ValidationFailed, JobFailed)
DEFAULT_MAX_ATTEMPTS = 5
# Steps retried without limit: the IRI job already exists, querying it is harmless, and giving
# up would leave the job running untracked (no stage-out, no cleanup). `cancel` stops such a run.
UNLIMITED_RETRY_STEPS = ("monitor",)


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _epoch(iso):
    return datetime.fromisoformat(iso).timestamp() if iso else None


class RunBusy(Exception):
    """Another process holds this run's lock."""


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

    @property
    def run_id(self):
        return self.data.get("run_id")

    @property
    def status(self):
        return self.data.get("status")

    def save(self):
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.data["updated_at"] = _now()
        RunIndex(self.run_dir.parent).save(self.data)

    def step(self, name):
        return self.data["steps"].setdefault(name, {"status": "pending"})

    def done(self, name):
        return self.step(name).get("status") in STEP_PASSED

    def set(self, name, status, **extra):
        st = self.step(name)
        if st.get("status") != status:
            reason = extra.get("reason") or extra.get("error")
            log.info("run %s: step %s %s -> %s%s", self.run_id, name, st.get("status"), status,
                     f" ({reason})" if reason else "")
        st.update(extra)
        st["status"] = status
        st["updated_at"] = _now()
        if status in STEP_PASSED or status == "failed":
            st.pop("attempts", None)
        self.save()

    def finish(self, status, message=None):
        self.data["status"] = status
        self.data["current_step"] = None
        self.data["finished_at"] = _now()
        if message:
            self.data["message"] = message
        log.info("run %s: %s%s", self.run_id, status, f": {message}" if message else "")
        self.save()


@contextmanager
def run_lock(run_dir, *, blocking=False):
    """Exclusive lock on a run. Raises RunBusy if not blocking and the run is held."""
    run_dir.mkdir(parents=True, exist_ok=True)
    fh = open(run_dir / ".lock", "a")
    try:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            raise RunBusy(run_dir.name) from None
        yield
    finally:
        fh.close()


# ---------------------------------------------------------------------- job helpers
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
    return bool(spec and (spec.get("manifest") or spec.get("rucio") or spec.get("files")))


def file_pairs(spec):
    """The job file's inline `files:` list as (SRC, DST) pairs.
    Each entry is "SRC DST" or {src: ..., dst: ...}."""
    out = []
    for item in (spec or {}).get("files") or []:
        if isinstance(item, dict):
            src, dst = item.get("src"), item.get("dst")
        else:
            parts = str(item).split()
            src, dst = (parts + [None, None])[:2] if len(parts) == 2 else (None, None)
        if not src or not dst:
            raise ConfigError(f"staging.e2e files entry must be 'SRC DST' or {{src, dst}}, got: {item}")
        out.append((src, dst))
    return out


def _e2e_sources(config, spec):
    manifest = config.paths.resolve(spec.get("manifest")) if spec.get("manifest") else None
    return manifest, spec.get("rucio") or [], file_pairs(spec)


# ---------------------------------------------------------------------- create
def create(config, job, site, *, run_id=None, do_validate=True, stagein_manifest=None, stageout_manifest=None,
           max_attempts=DEFAULT_MAX_ATTEMPTS):
    """Create a run (state.json + job.yaml snapshot). Nothing is contacted. Returns RunState."""
    _apply_manifest_overrides(job, stagein_manifest, stageout_manifest)
    job._check()
    for direction in ("stagein", "stageout"):
        file_pairs(job.e2e.get(direction))  # fail early on a bad files: entry
    run_id = run_id or jobs.new_run_id()
    run_dir = config.paths.run_dir(run_id)
    with run_lock(run_dir, blocking=True), run_logging(run_log_path(config, run_id, job), run_id):
        if (run_dir / "state.json").exists():
            raise ConfigError(f"run {run_id} already exists; use `globusiri monitor {run_id}` or `run --resume`")
        log.info("run %s: created at %s (job %s)", run_id, site.name, job.source or job.name)
        state = RunState(run_dir)
        state.data.update({"run_id": run_id, "site": site.name, "status": ACTIVE, "current_step": STEPS[0],
                           "created_at": _now(), "remote_work_dir": site.remote_run_dir(run_id),
                           "options": {"validate": bool(do_validate), "max_attempts": int(max_attempts)}})
        job.snapshot(run_dir / "job.yaml")
        state.save()
    return state


def load(config, run_id):
    """(RunState, Job, Site) of an existing run."""
    run_dir = config.paths.run_dir(run_id)
    state = RunState.load(run_dir)
    snap = yaml.safe_load((run_dir / "job.yaml").read_text())
    job, site = resolve_job(config, snapshot_data=snap)
    return state, job, site


# ---------------------------------------------------------------------- engine
class _Ctx:
    """Clients and per-pass data, created lazily so a pass that only polls one thing
    only builds the client it needs."""

    def __init__(self, config, job, site, state, echo):
        self.config, self.job, self.site, self.state = config, job, site, state
        self._echo = echo
        self._iri = self._https = self._transfer = None
        self.report = None  # IRI job report from this pass

    @property
    def run_id(self):
        return self.state.run_id

    def echo(self, msg):
        """Progress message for the caller; also logged (and so in the run log)."""
        log.info("run %s: %s", self.run_id, msg.strip())
        self._echo(msg)

    @property
    def iri(self):
        if self._iri is None:
            self._iri = services.iri_client(self.config, self.site)
        return self._iri

    @property
    def https(self):
        if self._https is None and self.site.has_https and (
                self.site.upload_method == "globus_https" or self.job.wants_https_download()):
            self._https = services.https_client(self.config, self.site)
        return self._https

    @property
    def transfer(self):
        if self._transfer is None:
            self._transfer = services.transfer(self.config)
        return self._transfer


def _poll_tasks(ctx, step, key="task_ids"):
    """Check Globus tasks recorded in a step. True when all SUCCEEDED, False while any is
    active; raises JobFailed if one failed or the e2e timeout passed."""
    st = ctx.state.step(step)
    infos = {tid: e2e_staging.task_info(ctx.transfer, tid) for tid in st[key]}
    bad = {tid: i for tid, i in infos.items() if i["status"] == "FAILED"}
    if bad:
        raise JobFailed(f"{step}: transfer failed: " + "; ".join(
            f"{tid}: {i.get('nice_status') or i['status']} {i.get('nice_status_details') or ''}".strip()
            for tid, i in bad.items()))
    if all(i["status"] == "SUCCEEDED" for i in infos.values()):
        return True
    started = _epoch(st.get("started_at"))
    if started and time.time() - started > ctx.config.e2e.timeout:
        raise JobFailed(f"{step}: tasks {', '.join(st[key])} not done after {ctx.config.e2e.timeout}s")
    st["checked_at"] = _now()
    tasks = {tid: {"status": i["status"], "files": i.get("files"),
                   "files_transferred": i.get("files_transferred"), "nice_status": i.get("nice_status")}
             for tid, i in infos.items()}
    if tasks != st.get("tasks"):
        ctx.echo(f"  {step}: " + "; ".join(
            f"{tid} {t['nice_status'] or t['status']} {t['files_transferred'] or 0}/{t['files'] or '?'} files"
            for tid, t in tasks.items()))
    st["tasks"] = tasks
    ctx.state.save()
    return False


def _step_validate(ctx):
    if not ctx.state.data.get("options", {}).get("validate", True):
        ctx.state.set("validate", "skipped", reason="--no-validate")
        return True
    checks = validate(ctx.config, job=ctx.job)
    for c in checks:
        if c.status != "OK":
            ctx.echo(f"  validate {c.kind:5} {c.check:14} {c.status:7} {c.detail_with_fix}")
    if failed(checks):
        ctx.state.set("validate", "failed", failures=[c.to_dict() for c in checks if c.status == FAIL])
        raise ValidationFailed("token validation failed; nothing was transferred or submitted")
    ctx.state.set("validate", "done")
    return True


def _e2e_transfer_step(ctx, step, direction):
    spec = ctx.job.e2e.get(direction) or {}
    st = ctx.state.step(step)
    if not st.get("task_ids"):
        manifest, rucio, pairs = _e2e_sources(ctx.config, spec)
        p = e2e_staging.plan(ctx.config, ctx.site, direction, manifest=manifest, rucio=rucio, pairs=pairs)
        suffix = "IN" if direction == "stagein" else "OUT"
        task_ids = e2e_staging.submit(ctx.transfer, ctx.config, p, label=f"globusiri-{ctx.run_id}-{suffix}")
        ctx.state.set(step, "running", task_ids=task_ids, files=len(p.items), started_at=_now())
        ctx.echo(f"  e2e {direction}: submitted {', '.join(task_ids)} ({len(p.items)} files)")
    if not _poll_tasks(ctx, step):
        return False
    ctx.state.set(step, "done")
    ctx.echo(f"  e2e {direction}: done")
    return True


def _step_e2e_stagein(ctx):
    if not (ctx.job.e2e_enabled and _has_files(ctx.job.e2e.get("stagein"))):
        ctx.state.set("e2e_stagein", "skipped", reason="job does not enable e2e stagein")
        return True
    return _e2e_transfer_step(ctx, "e2e_stagein", "stagein")


def _step_upload(ctx):
    sub, _ = jobs.prepare(ctx.config, ctx.job, ctx.site, ctx.run_id, ctx.state.run_dir)
    for s in sub.skipped_inputs:
        ctx.echo(f"  input skipped (not found): {s}")
    jobs.upload_inputs(ctx.config, ctx.site, sub, ctx.iri, ctx.https)
    ctx.state.set("upload", "done", submission=sub.to_dict(), method=ctx.site.upload_method)
    ctx.echo(f"  uploaded {', '.join(sub.uploaded)} to {sub.remote_dir} via {ctx.site.upload_method}")
    return True


def _submission(ctx):
    return jobs.Submission(**ctx.state.step("upload")["submission"])


def _step_submit(ctx):
    job_id = jobs.launch(ctx.site, _submission(ctx), ctx.iri)
    ctx.state.set("submit", "done", job_id=job_id, submitted_at=_now())
    ctx.echo(f"  submitted IRI job {job_id}")
    return True


def _step_monitor(ctx):
    job_id = ctx.state.step("submit")["job_id"]
    st, ctx.report = jobs.status(ctx.iri, ctx.site, job_id)
    prev = ctx.state.step("monitor").get("state")
    if st != prev:
        ctx.echo(f"  job {job_id}: {st}")
    if st in jobs.TERMINAL:
        ctx.state.set("monitor", "done", state=st,
                      exit_code=(ctx.report.get("status") or {}).get("exit_code"))
        return True
    ctx.state.set("monitor", "running", state=st, checked_at=_now())
    return False


def _step_https_stageout(ctx):
    if not (ctx.job.wants_https_download() and ctx.https is not None):
        ctx.state.set("https_stageout", "skipped", reason="job does not download over HTTPS")
        return True
    if ctx.report is None:
        _, ctx.report = jobs.status(ctx.iri, ctx.site, ctx.state.step("submit")["job_id"])
    h = ctx.job.https_stageout
    files = h.get("files") or []
    if h.get("stdout_stderr"):
        res = https_staging.stageout_job_logs(ctx.https, ctx.report, ctx.site, ctx.state.run_dir, run_id=ctx.run_id,
                                              extra_files=files)
    else:
        remote_dir = _submission(ctx).remote_dir
        res = https_staging.stageout(ctx.https, [posixpath.join(remote_dir, f) for f in files], ctx.state.run_dir)
    errors = {k: v for k, v in res.items() if str(v).startswith("error")}
    # a failed log download does not fail the run; it is recorded and the run moves on
    ctx.state.set("https_stageout", "done" if not errors else "failed", files=res)
    for remote, local in res.items():
        ctx.echo(f"  download {remote} -> {local}")
    return True


def _final(ctx):
    return ctx.state.step("monitor").get("state")


def _step_e2e_stageout(ctx):
    spec = ctx.job.e2e.get("stageout") or {}
    if not (ctx.job.e2e_enabled and _has_files(spec)):
        ctx.state.set("e2e_stageout", "skipped", reason="job does not enable e2e stageout")
        return True
    if _final(ctx) != jobs.FINISHED:
        ctx.state.set("e2e_stageout", "skipped", reason=f"job ended {_final(ctx)}")
        return True
    return _e2e_transfer_step(ctx, "e2e_stageout", "stageout")


def _step_cleanup(ctx):
    job, state = ctx.job, ctx.state
    want = job.cleanup
    in_spec = job.e2e.get("stagein") or {}
    do_e2e = bool(want.get("e2e_stagein")) and job.e2e_enabled and _has_files(in_spec)
    do_run = bool(want.get("remote_run_dir"))
    if not (do_e2e or do_run):
        state.set("cleanup", "skipped", reason="job does not enable cleanup")
        return True
    if _final(ctx) != jobs.FINISHED:
        state.set("cleanup", "skipped", reason=f"job ended {_final(ctx)}; files kept for debugging")
        return True
    bad = [s for s in ("https_stageout", "e2e_stageout") if not state.done(s)]
    if bad:
        state.set("cleanup", "skipped", reason=f"{', '.join(bad)} not done; files kept")
        return True
    st = state.step("cleanup")
    if do_e2e:
        if not st.get("e2e_task_ids"):
            manifest, rucio, pairs = _e2e_sources(ctx.config, in_spec)
            p = e2e_staging.cleanup_plan(ctx.site, manifest=manifest, rucio=rucio, paths=[d for _, d in pairs])
            task_ids = e2e_staging.submit_delete(ctx.transfer, ctx.config, p, label=f"globusiri-{ctx.run_id}-CLEANUP")
            state.set("cleanup", "running", e2e_task_ids=task_ids, e2e_files=len(p.items), started_at=_now())
            ctx.echo(f"  cleanup e2e stage-in files: submitted {', '.join(task_ids)}")
        if not _poll_tasks(ctx, "cleanup", key="e2e_task_ids"):
            return False
    if do_run:
        removed = cleanup_mod.cleanup_run(ctx.iri, ctx.site, ctx.run_id)
        for path, what in removed.items():
            ctx.echo(f"  cleanup {path}: {what}")
        st["removed"] = removed
    state.set("cleanup", "done")
    return True


HANDLERS = {
    "validate": _step_validate,
    "e2e_stagein": _step_e2e_stagein,
    "upload": _step_upload,
    "submit": _step_submit,
    "monitor": _step_monitor,
    "https_stageout": _step_https_stageout,
    "e2e_stageout": _step_e2e_stageout,
    "cleanup": _step_cleanup,
}


def _advance_locked(config, state, job, site, *, echo, raise_errors):
    if state.status in RUN_TERMINAL:
        return state
    ctx = _Ctx(config, job, site, state, echo)
    max_attempts = int(state.data.get("options", {}).get("max_attempts", DEFAULT_MAX_ATTEMPTS))
    for name in STEPS:
        st = state.step(name)
        if st.get("status") in STEP_PASSED or st.get("status") == "failed":
            continue
        if state.data.get("current_step") != name:
            state.data["current_step"] = name
            state.save()
        try:
            passed = HANDLERS[name](ctx)
            # the step's check went through: earlier transient errors no longer count
            cleared = state.data.pop("last_error", None) is not None
            cleared = state.step(name).pop("attempts", None) is not None or cleared
            if cleared:
                state.save()
            if not passed:
                return state  # step still in progress; the next pass checks it again
        except FATAL as exc:
            if st.get("status") != "failed":
                state.set(name, "failed", error=str(exc))
            state.finish(FAILED, f"{name}: {exc}")
            log.error("run %s failed at %s: %s", state.run_id, name, exc)
            if raise_errors:
                raise
            echo(f"  {name} failed: {exc}")
            return state
        except Exception as exc:  # transient: retry on a later pass
            attempts = int(st.get("attempts", 0)) + 1
            st["attempts"] = attempts
            state.data["last_error"] = {"step": name, "error": f"{type(exc).__name__}: {exc}", "at": _now(),
                                        "attempt": attempts}
            state.save()
            unlimited = name in UNLIMITED_RETRY_STEPS
            limit = "no limit" if unlimited else max_attempts
            log.warning("run %s step %s attempt %d/%s: %s", state.run_id, name, attempts, limit, exc)
            if raise_errors or (not unlimited and attempts >= max_attempts):
                state.set(name, "failed", error=str(exc))
                state.finish(FAILED, f"{name}: {exc} (after {attempts} attempts)")
                if raise_errors:
                    raise
            echo(f"  {name}: error (attempt {attempts}/{limit}): {exc}")
            return state
    final = _final(ctx)
    if final == jobs.FINISHED:
        state.finish(FINISHED)
        echo(f"run {state.run_id} finished")
    else:
        state.finish(FAILED, f"IRI job {state.step('submit').get('job_id')} ended {final}")
        echo(f"run {state.run_id} failed: job ended {final}")
    return state


def advance(config, run_id, *, echo=None, raise_errors=False, wait_lock=False):
    """One non-blocking pass over a run: check the current step and trigger the next ones.

    Returns the RunState. Raises RunBusy if another process is advancing this run (unless
    wait_lock). With raise_errors, a step error is re-raised after it is recorded
    (blocking mode); otherwise errors are recorded in state.json and transient ones are
    retried on the next pass (plugin and daemon modes).
    """
    echo = echo or (lambda *a: None)
    run_dir = config.paths.run_dir(run_id)
    with run_lock(run_dir, blocking=wait_lock):
        state, job, site = load(config, run_id)
        if not state.status:  # a run made by `job submit`, which the engine does not drive
            raise ConfigError(f"run {run_id} was not created by `globusiri submit` or `run`")
        if state.status in RUN_TERMINAL:
            return state
        with run_logging(run_log_path(config, run_id, job), run_id):
            return _advance_locked(config, state, job, site, echo=echo, raise_errors=raise_errors)


def list_runs(config, *, status=None):
    """Load only the runs selected by the central index, newest first."""
    out = []
    for row in RunIndex(config.paths.runs_dir).query(status=status):
        try:
            st = RunState.load(config.paths.run_dir(row["run_id"]))
        except (OSError, ValueError, ConfigError):
            continue
        if status is None or st.status in ((status,) if isinstance(status, str) else status):
            out.append(st)
    out.sort(key=lambda s: s.data.get("created_at") or "", reverse=True)
    return out


def monitor(config, run_ids=None, *, echo=None):
    """One pass over the given runs (default: every active run). Each run is checked and
    its next steps triggered; a run held by another process is skipped. Never raises for a
    single run. Returns {run_id: RunState or error string}."""
    echo = echo or (lambda *a: None)
    ids = list(run_ids) if run_ids is not None else [
        r["run_id"] for r in RunIndex(config.paths.runs_dir).query(status=ACTIVE, oldest_first=True)]
    out = {}
    for rid in ids:
        try:
            out[rid] = advance(config, rid, echo=echo)
        except RunBusy:
            out[rid] = "busy (another process is advancing it)"
        except Exception as exc:  # keep going with the other runs
            log.exception("monitor %s", rid)
            out[rid] = f"error: {exc}"
    return out


def daemon(config, *, interval=60, echo=None, stop=None, max_passes=None):
    """Call monitor() every `interval` seconds until stop() returns True (or max_passes).
    Only one daemon per WORK_DIR: a second one raises ConfigError."""
    echo = echo or (lambda *a: None)
    stop = stop or (lambda: False)
    lock_path = config.paths.runs_dir / ".daemon.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+")
    try:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ConfigError(f"another globusiri daemon is running for {config.paths.root}") from None
        fh.seek(0)
        fh.truncate()
        fh.write(f"{os.getpid()}\n")
        fh.flush()
        passes = 0
        while not stop():
            results = monitor(config, echo=echo)
            passes += 1
            if results:
                summary = {}
                for r in results.values():
                    k = r.status if isinstance(r, RunState) else "error"
                    summary[k] = summary.get(k, 0) + 1
                echo(f"[{_now()}] pass {passes}: " + ", ".join(f"{v} {k}" for k, v in sorted(summary.items())))
            if max_passes and passes >= max_passes:
                break
            deadline = time.time() + interval
            while not stop() and time.time() < deadline:
                time.sleep(min(1.0, max(0.0, deadline - time.time())))
    finally:
        fh.close()


def cancel(config, run_id, *, echo=None):
    """Cancel a run: cancel the IRI job and any running Globus task, mark it cancelled."""
    echo = echo or (lambda *a: None)
    with run_lock(config.paths.run_dir(run_id), blocking=True):
        state, job, site = load(config, run_id)
        if state.status in RUN_TERMINAL:
            return state
        with run_logging(run_log_path(config, run_id, job), run_id):
            return _cancel_locked(_Ctx(config, job, site, state, echo))


def _cancel_locked(ctx):
    state, site, echo = ctx.state, ctx.site, ctx.echo
    for name in ("e2e_stagein", "e2e_stageout"):
        st = state.step(name)
        if st.get("status") == "running":
            for tid in st.get("task_ids") or []:
                try:
                    e2e_staging.cancel(ctx.transfer, tid)
                    echo(f"  cancelled transfer {tid}")
                except Exception as exc:
                    echo(f"  cancel transfer {tid}: {exc}")
            state.set(name, "failed", error="cancelled")
    if state.done("submit") and not state.done("monitor"):
        job_id = state.step("submit")["job_id"]
        jobs.cancel(ctx.iri, site, job_id)
        state.set("monitor", "failed", state=jobs.CANCELLED, error="cancelled")
        echo(f"  cancelled IRI job {job_id}")
    state.finish(CANCELLED, "cancelled by user")
    return state


def plugin_status(state_data):
    """One-word status for plugin callers: submitted (before the batch job runs), running
    (job running, or its outputs still being staged), finished, failed, cancelled. These are
    the same strings as Harvester's WorkSpec.ST_* values."""
    status = state_data.get("status")
    if status in (FINISHED, FAILED, CANCELLED):
        return status
    mon = (state_data.get("steps") or {}).get("monitor") or {}
    if mon.get("status") == "done" or mon.get("state") == jobs.RUNNING:
        return "running"
    return "submitted"


# ---------------------------------------------------------------------- blocking mode
def run(config, job, site, *, run_id=None, no_wait=False, do_validate=True, stagein_manifest=None,
        stageout_manifest=None, interval=60, echo=print, resume_state=None):
    """Create (or resume) a run and advance it until it ends. With no_wait, make one pass
    only. Raises on a failed step, and JobFailed if the IRI job did not finish."""
    if resume_state is None:
        state = create(config, job, site, run_id=run_id, do_validate=do_validate,
                       stagein_manifest=stagein_manifest, stageout_manifest=stageout_manifest)
    else:
        state = resume_state
    run_id = state.run_id
    echo(f"run {run_id} at {site.name} ({state.run_dir})")
    while True:
        state = advance(config, run_id, echo=echo, raise_errors=True, wait_lock=True)
        if state.status in RUN_TERMINAL or no_wait:
            break
        time.sleep(interval)
    if no_wait:  # one pass only; the outcome is in state.json
        if state.status == ACTIVE:
            echo(f"  not waiting; continue with: globusiri monitor {run_id}")
        return state
    if state.status == FAILED:
        raise JobFailed(state.data.get("message") or f"run {run_id} failed")
    return state


def resume(config, run_id, *, do_validate=True, **kwargs):
    """Re-open a run (failed steps go back to pending) and continue it in blocking mode."""
    with run_lock(config.paths.run_dir(run_id), blocking=True):
        state, job, site = load(config, run_id)
        state.data["status"] = ACTIVE
        state.data.pop("finished_at", None)
        state.data.pop("message", None)
        state.data.setdefault("options", {})["validate"] = bool(do_validate)
        for st in state.data["steps"].values():
            if st.get("status") == "failed":
                # retry from scratch; a failed transfer is resubmitted (Globus sync_level
                # skips files that already arrived). "running" steps keep their task ids.
                for k in ("task_ids", "e2e_task_ids", "attempts", "error", "tasks"):
                    st.pop(k, None)
                st["status"] = "pending"
        state.save()
    return run(config, job, site, resume_state=state, **kwargs)
