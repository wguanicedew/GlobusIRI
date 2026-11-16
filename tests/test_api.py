import pytest

import globusiri
from globusiri import GlobusIRI, jobs, services
from globusiri.api import init
from globusiri.errors import ConfigError, ValidationFailed

from .test_validate_workflow import FakeHttps, FakeIri, all_tokens


def test_package_exports_api():
    assert globusiri.GlobusIRI is GlobusIRI


def test_init_and_paths(tmp_path):
    res = init(tmp_path / "w", sites="ALCF_CRUX")
    assert (tmp_path / "w" / "config" / "sites.yaml") in res["written"]
    assert (tmp_path / "w" / "jobs" / "perlmutter_script_job.sh") in res["written"]  # script_file example
    assert init(tmp_path / "w")["written"] == []  # kept on second run
    with pytest.raises(ConfigError):
        init(tmp_path / "w2", sites=["nope"])
    g = GlobusIRI(work_dir=tmp_path / "w")
    assert list(g.config.sites) == ["ALCF_CRUX"]
    assert dict((r[0], r[2]) for r in g.paths_info())["config"] == "WORK_DIR"


def test_job_submit_dry_run(cfg):
    r = GlobusIRI(cfg).job_submit("perlmutter_example_job.yaml", set=["resources.n_node=2"], dry_run=True)
    assert r["job_spec"]["resources"]["node_count"] == 2
    assert r["script"].startswith("#!")


def test_job_submit_validation_failure_has_checks(cfg):
    with pytest.raises(ValidationFailed) as exc:
        GlobusIRI(cfg).job_submit("perlmutter_example_job.yaml")
    assert exc.value.checks and all(c.status != "OK" for c in exc.value.checks)


def test_token_status_collects_per_target(cfg):
    results = GlobusIRI(cfg).token_status("iri", all_sites=True, raise_errors=False)
    assert {r.target for r in results} == {"NERSC_Perlmutter (nersc)", "ALCF_CRUX (alcf)"}
    assert all(r.ok and not r.result.access_exists for r in results)
    with pytest.raises(ConfigError):
        GlobusIRI(cfg).token_status("iri")  # no site, no all_sites
    with pytest.raises(ConfigError):
        GlobusIRI(cfg).token_status("bogus", all_sites=True)


def test_fs_rm_and_job_cleanup(cfg, monkeypatch):
    iri = FakeIri(["completed"])
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: iri)
    g = GlobusIRI(cfg)
    site = cfg.site("ALCF_CRUX")
    with pytest.raises(ConfigError, match="--force"):
        g.fs_rm("ALCF_CRUX", "/etc")
    assert g.fs_rm("ALCF_CRUX", site.remote_run_dir("R1") + "/f") is True
    assert g.job_cleanup("ALCF_CRUX", "R2") == {site.remote_run_dir("R2"): "removed"}
    assert [c for c in iri.calls if c[0] == "rm"] == [("rm", site.remote_run_dir("R1") + "/f"),
                                                      ("rm", site.remote_run_dir("R2"))]


def test_e2e_cleanup_dry_run_needs_no_token(cfg, monkeypatch):
    monkeypatch.setattr(services, "transfer", lambda c: pytest.fail("dry run must not contact Globus"))
    r = GlobusIRI(cfg).e2e_cleanup("ALCF_CRUX", rucio=["mc23:f1"], dry_run=True)
    assert r["endpoint"] == "Crux" and len(r["items"]) == 1 and "task_ids" not in r


def test_run_through_api(cfg, monkeypatch, work):
    all_tokens(cfg)
    iri, https = FakeIri(["completed"]), FakeHttps(200)
    monkeypatch.setattr(services, "iri_client", lambda c, s, **kw: iri)
    monkeypatch.setattr(services, "https_client", lambda c, s, **kw: https)
    monkeypatch.setattr(jobs.time, "sleep", lambda s: None)
    g = GlobusIRI(cfg)
    lines = []
    state = g.run("crux_example_job.yaml", run_id="A1", validate=False, interval=0, echo=lines.append)
    assert state.data["steps"]["monitor"]["state"] == jobs.FINISHED
    assert g.run_state("A1")["steps"]["submit"]["job_id"] == "JOB42"
    assert any("finished" in line for line in lines)
