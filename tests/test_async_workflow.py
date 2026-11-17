#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Non-blocking workflow: submit once, then each monitor() pass checks the current step
and triggers the next (plugin mode); daemon() repeats the passes."""

import pytest

from globusiri import services, workflow
from globusiri.api import GlobusIRI
from globusiri.errors import ConfigError
from globusiri.staging import e2e as e2e_staging

from .test_validate_workflow import FakeHttps, FakeIri, all_tokens

IN_FILE = "/pnfs/usatlas.bnl.gov/atlasdatadisk/rucio/mc23/EVNT.root /global/cfs/cdirs/m2616/harvester_workdir/rucio/EVNT.root"
OUT_FILE = {"src": "/global/cfs/cdirs/m2616/harvester_workdir/out/AOD.root",
            "dst": "/pnfs/usatlas.bnl.gov/atlasdatadisk/rucio/out/AOD.root"}


class FakeGlobus:
    """Globus Transfer tasks whose status the test controls."""

    def __init__(self):
        self.tasks = {}  # task_id -> status
        self.submitted = []
        self.cancelled = []

    def submit(self, transfer, config, p, *, label=None, **kw):
        tid = f"T{len(self.tasks) + 1}"
        self.tasks[tid] = "ACTIVE"
        self.submitted.append((p.direction, label, list(p.items)))
        return [tid]

    def task_info(self, transfer, tid):
        return {"task_id": tid, "status": self.tasks[tid]}

    def cancel(self, transfer, tid):
        self.cancelled.append(tid)


@pytest.fixture
def env(cfg, monkeypatch, work):
    all_tokens(cfg)
    iri, https, gl = FakeIri(["queued"]), FakeHttps(200), FakeGlobus()
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: iri)
    monkeypatch.setattr(services, "https_client", lambda c, s, **kw: https)
    monkeypatch.setattr(services, "transfer", lambda c: object())
    monkeypatch.setattr(e2e_staging, "submit", gl.submit)
    monkeypatch.setattr(e2e_staging, "task_info", gl.task_info)
    monkeypatch.setattr(e2e_staging, "cancel", gl.cancel)
    g = GlobusIRI(cfg)
    return g, iri, gl


E2E_JOB = ["staging.e2e.enabled=true", f"staging.e2e.stagein.files=['{IN_FILE}']",
           "staging.e2e.stageout.files=[{src: %s, dst: %s}]" % (OUT_FILE["src"], OUT_FILE["dst"])]


def steps(r):
    return {k: v["status"] for k, v in r["steps"].items()}


def test_monitor_passes_drive_the_whole_workflow(env):
    g, iri, gl = env
    r = g.submit("perlmutter_example_job.yaml", set=E2E_JOB, run_id="A1", validate=False)
    # first pass: e2e stage-in submitted, nothing on IRI yet
    assert r["status"] == "active" and r["current_step"] == "e2e_stagein"
    assert steps(r)["e2e_stagein"] == "running" and not iri.calls
    assert gl.submitted[0][0] == "stagein"
    # BNL maps /pnfs -> /dcint-door003; NERSC has no path_map
    src, dst = IN_FILE.split()
    assert gl.submitted[0][2] == [(src.replace("/pnfs/", "/dcint-door003/", 1), dst)]

    # transfer still active: monitor does nothing more
    r = g.monitor()["A1"]
    assert r["current_step"] == "e2e_stagein" and not iri.calls

    # transfer done: next pass uploads, submits and sees the job queued
    gl.tasks["T1"] = "SUCCEEDED"
    r = g.monitor()["A1"]
    assert steps(r)["upload"] == "done" and r["steps"]["submit"]["job_id"] == "JOB42"
    assert r["current_step"] == "monitor" and r["plugin_status"] == "submitted"
    launches = [c for c in iri.calls if c[0] == "launch"]

    iri.states = ["active"]
    r = g.monitor()["A1"]
    assert r["plugin_status"] == "running"

    # job completes: logs downloaded, stage-out submitted, run still active
    iri.states = ["completed"]
    r = g.monitor()["A1"]
    assert steps(r)["https_stageout"] == "done" and steps(r)["e2e_stageout"] == "running"
    assert r["status"] == "active" and r["plugin_status"] == "running"
    assert gl.submitted[1][0] == "stageout"

    gl.tasks["T2"] = "SUCCEEDED"
    r = g.monitor()["A1"]
    assert r["status"] == "finished" and r["plugin_status"] == "finished"
    assert [c for c in iri.calls if c[0] == "launch"] == launches  # never resubmitted

    # finished runs are no longer picked up
    assert g.monitor() == {}


def test_failed_stagein_fails_run_before_submit(env):
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", set=E2E_JOB, run_id="A2", validate=False)
    gl.tasks["T1"] = "FAILED"
    r = g.monitor()["A2"]
    assert r["status"] == "failed" and steps(r)["e2e_stagein"] == "failed"
    assert "transfer failed" in r["message"]
    assert not [c for c in iri.calls if c[0] == "launch"]


def test_transient_error_is_retried_then_fails(env, monkeypatch):
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", set=E2E_JOB, run_id="A3", validate=False, max_attempts=2)

    def boom(transfer, tid):
        raise RuntimeError("globus 502")

    monkeypatch.setattr(e2e_staging, "task_info", boom)
    r = g.monitor()["A3"]
    assert r["status"] == "active" and r["last_error"]["attempt"] == 1
    monkeypatch.setattr(e2e_staging, "task_info", gl.task_info)
    gl.tasks["T1"] = "SUCCEEDED"
    r = g.monitor()["A3"]  # recovered: the error is cleared and the run moves on
    assert "last_error" not in r and r["current_step"] == "monitor"

    # the IRI job exists: status-query errors never end the run, however many there are
    get_job = iri.get_job
    monkeypatch.setattr(iri, "get_job", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("iri down")))
    for _ in range(5):
        r = g.monitor()["A3"]
    assert r["status"] == "active" and r["last_error"]["attempt"] == 5 and r["current_step"] == "monitor"
    monkeypatch.setattr(iri, "get_job", get_job)
    r = g.monitor()["A3"]
    assert "last_error" not in r and "attempts" not in r["steps"]["monitor"]


def test_locked_run_is_skipped(env, work):
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", set=E2E_JOB, run_id="A4", validate=False)
    with workflow.run_lock(work.run_dir("A4")):
        assert "busy" in g.monitor()["A4"]
        with pytest.raises(workflow.RunBusy):
            g.advance("A4")


def test_cancel_cancels_transfer_and_job(env):
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", set=E2E_JOB, run_id="A5", validate=False)
    r = g.cancel("A5")
    assert r["status"] == "cancelled" and gl.cancelled == ["T1"]

    cancelled = []
    iri.cancel_job = lambda job_id, **kw: cancelled.append(job_id)
    g.submit("perlmutter_example_job.yaml", run_id="A6", validate=False)  # no e2e: submitted in pass 1
    r = g.cancel("A6")
    assert cancelled == ["JOB42"] and r["status"] == "cancelled"
    assert g.monitor() == {}


def test_daemon_runs_passes_until_stopped(env):
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", run_id="A7", validate=False)
    iri.states = ["completed"]
    lines = []
    g.daemon(interval=0, echo=lines.append, max_passes=1)
    assert g.run_state("A7")["status"] == "finished"
    assert any("pass 1" in ln for ln in lines)


def test_second_daemon_refused(env, work):
    import fcntl

    g, iri, gl = env
    work.runs_dir.mkdir(parents=True, exist_ok=True)
    with open(work.runs_dir / ".daemon.lock", "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ConfigError, match="another globusiri daemon"):
            g.daemon(interval=0, max_passes=1)


def test_runs_listing_and_overrides(env):
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", run_id="A8", validate=False, advance=False,
             overrides={"inputs": {"pandaJobData.out": "/nonexistent/pandaJobData.out"}})
    assert [r["run_id"] for r in g.runs(status="active")] == ["A8"]
    assert g.run_state("A8")["current_step"] == "validate"


def test_bad_inline_file_rejected(env):
    g, iri, gl = env
    with pytest.raises(ConfigError, match="SRC DST"):
        g.submit("perlmutter_example_job.yaml", set=["staging.e2e.enabled=true", "staging.e2e.stagein.files=[onlyone]"],
                 validate=False)


def test_monitor_all_runs_uses_index_and_picks_up_new_submissions(env, work, monkeypatch):
    from globusiri.runindex import RunIndex

    g, iri, gl = env
    for rid, job in (("many1", "perlmutter_hello_job.yaml"), ("many2", "crux_hello_job.yaml")):
        g.submit(job, run_id=rid, validate=False)
    assert set(g.monitor()) == {"many1", "many2"}
    assert g.monitor([]) == {}
    iri.states = ["completed"]
    assert all(r["status"] == "finished" for r in g.monitor().values())

    g.submit("perlmutter_hello_job.yaml", run_id="later", validate=False, advance=False)
    loaded = []
    original = workflow.RunState.load.__func__

    def track(cls, run_dir):
        loaded.append(run_dir.name)
        return original(cls, run_dir)

    def no_scan(*args):
        pytest.fail("monitor should query the index, not scan finished runs")

    monkeypatch.setattr(workflow.RunState, "load", classmethod(track))
    monkeypatch.setattr(RunIndex, "_scan_into", no_scan)
    assert set(g.monitor()) == {"later"}
    assert loaded == ["later"]
    loaded.clear()
    assert len(g.runs(summary=True)) == 3
    assert loaded == []


def test_submit_succeeds_when_monitor_takes_first_pass(env, monkeypatch):
    g, iri, gl = env

    def busy(*args, **kwargs):
        raise workflow.RunBusy("picked up by monitor")

    monkeypatch.setattr(workflow, "advance", busy)
    assert g.submit("perlmutter_hello_job.yaml", run_id="race", validate=False)["status"] == "active"


def test_duplicate_run_does_not_overwrite_snapshot(env, work):
    g, iri, gl = env
    g.submit("perlmutter_hello_job.yaml", run_id="same", validate=False, advance=False)
    before = (work.run_dir("same") / "job.yaml").read_text()
    with pytest.raises(ConfigError, match="already exists"):
        g.submit("crux_hello_job.yaml", run_id="same", validate=False)
    assert (work.run_dir("same") / "job.yaml").read_text() == before


def test_run_log_gets_job_events(env, work):
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", set=E2E_JOB, run_id="L1", validate=False)
    gl.tasks["T1"] = "SUCCEEDED"
    g.monitor()
    iri.states = ["completed"]
    g.monitor()
    text = (work.runs_dir / "L1" / "globusiri.log").read_text()
    assert "run L1: created" in text
    assert "e2e stagein: submitted T1" in text
    assert "globusiri.jobs: submitted IRI job JOB42" in text  # module logger, not only workflow
    assert "job JOB42: finished" in text  # monitor state change
    assert "step https_stageout pending -> done" in text


def test_run_log_path_from_job_and_isolated_between_runs(env, work):
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", set=["log=logs/runs/{run_id}.log"], run_id="L2", validate=False)
    g.submit("perlmutter_example_job.yaml", run_id="L3", validate=False)
    l2 = (work.root / "logs" / "runs" / "L2.log").read_text()
    assert "run L2:" in l2 and "L3" not in l2
    assert not (work.runs_dir / "L2" / "globusiri.log").exists()
    assert "L2" not in (work.runs_dir / "L3" / "globusiri.log").read_text()


def test_failed_run_resumed_with_monitor_or_submit(env, capsys):
    from globusiri.cli import build_parser, cmd_submit

    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", set=E2E_JOB, run_id="R1", validate=False)
    gl.tasks["T1"] = "FAILED"
    assert g.monitor()["R1"]["status"] == "failed"
    assert g.monitor(["R1"])["R1"]["status"] == "failed"  # a plain monitor leaves it failed
    with pytest.raises(ConfigError, match="needs RUN_ID"):
        g.monitor(resume=True)

    # monitor --resume: the failed stage-in goes back to pending and is submitted again
    r = g.monitor(["R1"], resume=True)["R1"]
    assert r["status"] == "active" and steps(r)["e2e_stagein"] == "running" and len(gl.submitted) == 2
    assert steps(r)["validate"] == "skipped"  # done/skipped steps are kept

    gl.tasks["T2"] = "FAILED"
    assert g.monitor()["R1"]["status"] == "failed"

    # submit --resume does the same from the CLI
    args = build_parser().parse_args(["submit", "--resume", "R1", "--no-validate"])
    assert cmd_submit(args, g) == 0
    assert "R1" in capsys.readouterr().out
    r = g.run_state("R1")
    assert r["status"] == "active" and len(gl.submitted) == 3 and r["options"]["validate"] is False

    with pytest.raises(ConfigError, match="not both"):
        cmd_submit(build_parser().parse_args(["submit", "--resume", "R1", "--job", "x.yaml"]), g)
    with pytest.raises(ConfigError, match="needs --job"):
        cmd_submit(build_parser().parse_args(["submit"]), g)


def test_rucio_deterministic_job_reaches_globus_once(env):
    """staging.e2e scope/rucio_deterministic: the workflow resolves each DST exactly once."""
    g, iri, gl = env
    g.submit("perlmutter_example_job.yaml", run_id="RD1", validate=False, overrides={"staging": {"e2e": {
        "enabled": True, "scope": "user.wguan", "rucio_deterministic": True,
        "stagein": {"files": [{"src": "/pnfs/usatlas.bnl.gov/atlasdatadisk/x/fileA",
                               "dst": "/global/cfs/cdirs/m2616/harvester_workdir/rucio"}]}}}})
    (src, dst), = gl.submitted[0][2]
    assert src == "/dcint-door003/usatlas.bnl.gov/atlasdatadisk/x/fileA"
    assert dst == "/global/cfs/cdirs/m2616/harvester_workdir/rucio/user/wguan/cb/6a/fileA"


def test_download_failed_then_monitor_retries_only_missing_files(env, monkeypatch):
    from globusiri.errors import RemoteError

    g, iri, gl = env
    https = services.https_client(None, None)
    real_get, missing = https.get, {"result.txt"}

    def get(remote, local):
        if remote.rsplit("/", 1)[-1] in missing:
            raise RemoteError(f"HTTPS GET {remote}: HTTP 404: No such file or directory")
        real_get(remote, local)

    https.get = get
    r = g.submit("perlmutter_hello_job.yaml", run_id="D1", validate=False,
                 set=["staging.https_stageout={stdout_stderr: true, files: [result.txt]}",
                      "cleanup.remote_run_dir=true"])
    iri.states = ["completed"]
    r = g.monitor(["D1"])["D1"]
    assert r["status"] == workflow.DOWNLOAD_FAILED and r["plugin_status"] == "finished"
    assert "result.txt" in r["message"] and "globusiri monitor D1" in r["message"]
    assert r["steps"]["https_stageout"]["status"] == "failed"
    assert r["steps"]["cleanup"]["status"] == "skipped"          # files kept until downloads work
    assert g.monitor() == {}                                      # passes over active runs skip it
    assert g.runs(status=workflow.DOWNLOAD_FAILED)[0]["run_id"] == "D1"

    fetched = len(https.gets)
    missing.clear()
    r = g.monitor(["D1"])["D1"]                                   # named explicitly: retried
    assert r["status"] == "finished", r.get("message")
    assert https.gets[fetched:] == [p for p in r["steps"]["https_stageout"]["files"] if p.endswith("result.txt")]
    assert not any(str(v).startswith("error") for v in r["steps"]["https_stageout"]["files"].values())
    assert r["steps"]["cleanup"]["status"] == "done"


def test_upload_failed_then_monitor_retries_upload(env):
    from globusiri.errors import RemoteError

    g, iri, gl = env
    https = services.https_client(None, None)
    real_put, broken = https.put, {"on": True}

    def put(local, remote):
        if broken["on"]:
            raise RemoteError("HTTPS PUT: HTTP 500: GlobusError")
        real_put(local, remote)

    https.put = put
    r = g.submit("perlmutter_hello_job.yaml", run_id="U1", validate=False, max_attempts=2)
    assert r["status"] == "active" and r["steps"]["upload"].get("attempts") == 1
    r = g.monitor()["U1"]
    assert r["status"] == workflow.UPLOAD_FAILED and r["plugin_status"] == "failed"
    assert r["steps"]["submit"]["status"] == "pending"
    assert not any(c[0] == "launch" for c in iri.calls)
    assert g.monitor() == {}

    broken["on"] = False
    r = g.monitor(["U1"])["U1"]
    assert r["status"] == "active" and r["steps"]["upload"]["status"] == "done"
    assert r["steps"]["submit"]["status"] == "done"
    assert any(c[0] == "launch" for c in iri.calls)


def _failing_downloads(names):
    from globusiri.errors import RemoteError

    https = services.https_client(None, None)
    real_get = https.get

    def get(remote, local):
        if remote.rsplit("/", 1)[-1] in names:
            raise RemoteError(f"HTTPS GET {remote}: HTTP 404: No such file or directory")
        real_get(remote, local)

    https.get = get
    return https


def _download_failed_run(g, iri, run_id):
    g.submit("perlmutter_hello_job.yaml", run_id=run_id, validate=False,
             set=["staging.https_stageout={stdout_stderr: true, files: [result.txt]}"])
    iri.states = ["completed"]
    return g.monitor([run_id])[run_id]


def test_download_failed_becomes_final_after_retries(env):
    g, iri, gl = env
    g.config.transfer_retries = 2
    https = _failing_downloads({"result.txt"})
    assert _download_failed_run(g, iri, "F1")["status"] == workflow.DOWNLOAD_FAILED
    r = g.monitor(["F1"])["F1"]                                    # retry 1 fails again
    assert r["status"] == workflow.DOWNLOAD_FAILED and r["transfer_retries"] == 1
    assert "retry 2/2" in r["message"]
    r = g.monitor(["F1"])["F1"]                                    # retry 2 fails: give up
    assert r["status"] == workflow.FINAL_DOWNLOAD_FAILED and r["plugin_status"] == "finished"
    assert "gave up after 2 retries" in r["message"]
    gets = len(https.gets)
    assert g.monitor(["F1"])["F1"]["status"] == workflow.FINAL_DOWNLOAD_FAILED
    assert len(https.gets) == gets                                 # no more attempts
    g.reopen("F1")                                                 # manual resume: counted again from 0
    assert g.run_state("F1").get("transfer_retries") is None


def test_monitor_all_retries_transfer_failed_runs_when_due(env):
    g, iri, gl = env
    missing = {"result.txt"}
    _failing_downloads(missing)
    assert _download_failed_run(g, iri, "T1")["status"] == workflow.DOWNLOAD_FAILED
    assert g.monitor() == {}                                       # not due yet (10 min)
    g.config.transfer_retry_interval = 0
    missing.clear()
    assert g.monitor()["T1"]["status"] == "finished"


def test_clean_old_runs(env, monkeypatch):
    g, iri, gl = env
    _failing_downloads(set())
    for rid in ("old-done", "old-dl"):
        g.submit("perlmutter_hello_job.yaml", run_id=rid, validate=False)
    g.submit("perlmutter_hello_job.yaml", run_id="still-active", validate=False)
    iri.states = ["completed"]
    g.monitor(["old-done", "old-dl"])
    st = g.run_state("old-dl")
    st["status"] = workflow.DOWNLOAD_FAILED                      # still retried by monitor: kept
    g.config.run_index().save(st)

    assert g.clean(older_than="1d") == []                        # nothing that old
    rows = g.clean(older_than=0, dry_run=True)
    assert [(r["run_id"], r["result"]) for r in rows] == [("old-done", "would remove")]
    assert g.config.paths.run_dir("old-done").is_dir()

    iri.calls.clear()
    rows = g.clean(older_than=0, remote=True)
    assert [(r["run_id"], r["result"]) for r in rows] == [("old-done", "removed")]
    assert any(c[0] == "rm" and c[1].endswith("/old-done") for c in iri.calls)
    assert not g.config.paths.run_dir("old-done").exists()
    assert {r["run_id"] for r in g.runs(summary=True)} == {"old-dl", "still-active"}

    rows = g.clean(older_than=0, status=[workflow.DOWNLOAD_FAILED])
    assert [r["run_id"] for r in rows] == ["old-dl"]
    with pytest.raises(ConfigError, match="unknown run status"):
        g.clean(older_than=0, status="done")
