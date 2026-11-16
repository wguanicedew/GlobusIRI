import time

import pytest

from globusiri import jobs, services, validate as V, workflow
from globusiri.errors import JobFailed, ValidationFailed
from globusiri.jobfile import resolve_job
from globusiri.tokens import manager

from .conftest import write_access


def all_tokens(cfg, expires_in=48 * 3600):
    exp = int(time.time()) + expires_in
    for site in cfg.sites.values():
        write_access(manager("iri", cfg, site).access_file, base_url=site.base_url, resource_id=site.resource_id,
                     expires_at=exp)
        write_access(manager("https", cfg, site).access_file, client_id="c", collection_id="C",
                     https_server="https://g", expires_at=exp)
    write_access(manager("e2e", cfg).access_file, client_id="c", expires_at=exp)


def statuses(checks):
    return {(c.site, c.kind, c.check): c.status for c in checks}


def test_offline_ok(cfg):
    all_tokens(cfg)
    checks = V.validate(cfg, offline=True)
    assert not V.failed(checks, strict=True)
    assert {c.kind for c in checks} == {"iri", "https", "e2e"}


def test_offline_expired_and_soon(cfg):
    all_tokens(cfg)
    write_access(manager("e2e", cfg).access_file, client_id="c", expires_at=int(time.time()) - 10)
    write_access(manager("https", cfg, cfg.site("ALCF_CRUX")).access_file, client_id="c", collection_id="C",
                 https_server="https://g", expires_at=int(time.time()) + 3600)
    s = statuses(V.validate(cfg, offline=True, min_lifetime=6 * 3600))
    assert s[("-", "e2e", "token file")] == V.FAIL
    assert s[("ALCF_CRUX", "https", "token file")] == V.WARN


def test_iri_base_url_mismatch_fails(cfg):
    all_tokens(cfg)
    site = cfg.site("ALCF_CRUX")
    write_access(manager("iri", cfg, site).access_file, base_url="https://wrong", resource_id=site.resource_id)
    s = statuses(V.validate(cfg, sites=[site], kinds=["iri"], offline=True))
    assert s[("ALCF_CRUX", "iri", "content")] == V.FAIL


def test_job_without_e2e_never_checks_e2e(cfg):
    all_tokens(cfg)
    manager("e2e", cfg).access_file.unlink()  # missing e2e token must not matter
    job, _ = resolve_job(cfg, "perlmutter_example_job.yaml")
    checks = V.validate(cfg, job=job, offline=True)
    assert not V.failed(checks)
    e2e = [c for c in checks if c.kind == "e2e"]
    assert len(e2e) == 1 and e2e[0].status == V.SKIP and "does not enable e2e" in e2e[0].detail

    job, _ = resolve_job(cfg, "perlmutter_example_job.yaml", overrides=["staging.e2e.enabled=true"])
    assert V.failed(V.validate(cfg, job=job, offline=True))


def test_kinds_cannot_add_to_job(cfg):
    all_tokens(cfg)
    job, _ = resolve_job(cfg, "perlmutter_example_job.yaml")
    checks = V.validate(cfg, job=job, kinds=["e2e"], offline=True)
    assert all(c.status == V.SKIP for c in checks)


class FakeHttps:
    def __init__(self, code):
        self.code = code
        self.puts, self.gets = [], []

    def probe(self, path):
        return self.code, "url"

    def put(self, local, remote):
        self.puts.append((str(local), remote))

    def get(self, remote, local):
        self.gets.append(remote)
        from pathlib import Path

        Path(local).parent.mkdir(parents=True, exist_ok=True)
        Path(local).write_text(f"content of {remote}")


@pytest.mark.parametrize("code,status", [(200, V.OK), (403, V.FAIL), (404, V.FAIL), (405, V.WARN)])
def test_https_live_codes(cfg, monkeypatch, code, status):
    monkeypatch.setattr(services, "https_client", lambda c, s, **kw: FakeHttps(code))
    (check,) = V.https_checks(cfg, cfg.site("ALCF_CRUX"))
    assert check.status == status
    if code == 404:
        assert "path_map" in check.detail
    if code == 403:
        assert check.fix == "globusiri https login ALCF_CRUX --force"


class FakeIri:
    def __init__(self, states):
        self.states = list(states)
        self.calls = []

    def mkdir(self, path, **kw):
        self.calls.append(("mkdir", path))

    def upload(self, local, remote, **kw):
        self.calls.append(("upload", remote))

    def chmod(self, path, mode, **kw):
        self.calls.append(("chmod", path, mode))

    def rm(self, path, **kw):
        self.calls.append(("rm", path))

    def launch_job(self, spec, **kw):
        self.calls.append(("launch", spec["name"]))
        return {"id": "JOB42"}

    def get_job(self, job_id, **kw):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return {"status": {"state": state, "exit_code": 0 if state == "completed" else None,
                           "meta_data": {"admincomment": '{"stdoutPath": "/o/out.txt", "stderrPath": "/o/err.txt"}'}}}


def test_run_end_to_end_without_e2e(cfg, monkeypatch, work):
    all_tokens(cfg)
    iri, https = FakeIri(["queued", "active", "completed"]), FakeHttps(200)
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: iri)
    monkeypatch.setattr(services, "https_client", lambda c, s, **kw: https)
    monkeypatch.setattr(services, "transfer", lambda c: pytest.fail("e2e must not be used"))
    monkeypatch.setattr(jobs.time, "sleep", lambda s: None)
    (work.inputs_dir / "panda_token").write_text("t")

    job, site = resolve_job(cfg, "crux_example_job.yaml")  # Crux uploads via globus_https
    state = workflow.run(cfg, job, site, run_id="R1", do_validate=False, interval=0, echo=lambda *a: None)
    steps = {k: v["status"] for k, v in state.data["steps"].items()}
    assert steps == {"validate": "skipped", "e2e_stagein": "skipped", "upload": "done", "submit": "done",
                     "monitor": "done", "https_stageout": "done", "e2e_stageout": "skipped",
                     "cleanup": "skipped"}
    remote_dir = "/lus/eagle/projects/ATLAS_workflow_ALCF/usatlas/IRI_workdir/jobs/R1"
    # only two uploads: the job script and one archive holding every input
    assert [p[1] for p in https.puts] == [f"{remote_dir}/executable_batch", f"{remote_dir}/globusiri_inputs.tar.gz"]
    assert ("chmod", f"{remote_dir}/executable_batch", "0755") in iri.calls
    assert ("upload",) not in [c[:1] for c in iri.calls]  # nothing uploaded via IRI on Crux
    assert https.gets == ["/o/out.txt", "/o/err.txt"]
    assert (work.run_dir("R1") / "out.txt").is_file()
    assert state.data["steps"]["submit"]["job_id"] == "JOB42"


def test_run_stops_before_anything_on_bad_token(cfg, monkeypatch):
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: pytest.fail("must not reach IRI"))
    job, site = resolve_job(cfg, "perlmutter_example_job.yaml")
    with pytest.raises(ValidationFailed):
        workflow.run(cfg, job, site, run_id="R2", echo=lambda *a: None)


def test_run_no_wait_then_resume_failed_job(cfg, monkeypatch, work):
    all_tokens(cfg)
    iri, https = FakeIri(["failed"]), FakeHttps(200)
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: iri)
    monkeypatch.setattr(services, "https_client", lambda c, s, **kw: https)
    monkeypatch.setattr(jobs.time, "sleep", lambda s: None)
    job, site = resolve_job(cfg, "perlmutter_example_job.yaml")
    workflow.run(cfg, job, site, run_id="R3", do_validate=False, no_wait=True, echo=lambda *a: None)
    launches = [c for c in iri.calls if c[0] == "launch"]
    with pytest.raises(JobFailed):
        workflow.resume(cfg, "R3", do_validate=False, interval=0, echo=lambda *a: None)
    assert [c for c in iri.calls if c[0] == "launch"] == launches  # not resubmitted
    st = workflow.RunState.load(work.run_dir("R3"))
    assert st.data["steps"]["monitor"]["state"] == jobs.FAILED
    assert st.data["steps"]["https_stageout"]["status"] == "done"  # logs still fetched


def test_run_cleanup_removes_remote_run_dir(cfg, monkeypatch, work):
    all_tokens(cfg)
    iri, https = FakeIri(["completed"]), FakeHttps(200)
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: iri)
    monkeypatch.setattr(services, "https_client", lambda c, s, **kw: https)
    monkeypatch.setattr(jobs.time, "sleep", lambda s: None)
    job, site = resolve_job(cfg, "crux_example_job.yaml", overrides=["cleanup.remote_run_dir=true"])
    state = workflow.run(cfg, job, site, run_id="R4", do_validate=False, interval=0, echo=lambda *a: None)
    assert ("rm", "/lus/eagle/projects/ATLAS_workflow_ALCF/usatlas/IRI_workdir/jobs/R4") in iri.calls
    assert state.data["steps"]["cleanup"]["status"] == "done"


def test_run_cleanup_skipped_when_job_failed(cfg, monkeypatch, work):
    all_tokens(cfg)
    iri, https = FakeIri(["failed"]), FakeHttps(200)
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: iri)
    monkeypatch.setattr(services, "https_client", lambda c, s, **kw: https)
    monkeypatch.setattr(jobs.time, "sleep", lambda s: None)
    job, site = resolve_job(cfg, "crux_example_job.yaml", overrides=["cleanup.remote_run_dir=true"])
    with pytest.raises(JobFailed):
        workflow.run(cfg, job, site, run_id="R5", do_validate=False, interval=0, echo=lambda *a: None)
    assert not [c for c in iri.calls if c[0] == "rm"]
    st = workflow.RunState.load(work.run_dir("R5"))
    assert st.data["steps"]["cleanup"]["status"] == "skipped"


class NoStatIri:
    """stat fails with the given status (501 at ALCF; NERSC's flaky task failure has none)."""

    def __init__(self, stat_status=501, ls_status=None):
        self.stat_status, self.ls_status = stat_status, ls_status
        self.calls = []

    def stat(self, path, **kw):
        from globusiri.errors import RemoteError

        self.calls.append("stat")
        raise RemoteError("IRI stat: failed", status=self.stat_status)

    def ls(self, path, **kw):
        from globusiri.errors import RemoteError

        self.calls.append("ls")
        if self.ls_status:
            raise RemoteError(f"IRI ls: HTTP {self.ls_status}", status=self.ls_status)
        return []


@pytest.mark.parametrize("stat_status,ls_status,expected,detail", [
    (501, None, V.OK, "stat not implemented"),      # ALCF
    (None, None, V.WARN, "stat failed"),            # NERSC: stat task failed, ls works
    (501, 501, V.WARN, "neither stat nor ls"),
    (501, 500, V.FAIL, "ls: IRI ls: HTTP 500"),
    (None, 500, V.FAIL, "ls: IRI ls: HTTP 500"),
])
def test_workdir_check_falls_back_to_ls(cfg, monkeypatch, stat_status, ls_status, expected, detail):
    iri = NoStatIri(stat_status, ls_status)
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: iri)
    check = V.iri_workdir_check(cfg, cfg.site("ALCF_CRUX"))
    assert iri.calls == ["stat", "ls"] and check.status == expected and detail in check.detail
