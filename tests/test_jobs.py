import pytest

from globusiri import jobs
from globusiri.errors import ConfigError
from globusiri.jobfile import parse_set, resolve_job
from globusiri import cleanup
from globusiri.errors import RemoteError
from globusiri.staging.e2e import cleanup_plan, plan, rucio_path


def test_parse_set():
    assert parse_set(["resources.n_node=4", "name=x", "staging.e2e.enabled=true"]) == {
        "resources": {"n_node": 4}, "name": "x", "staging": {"e2e": {"enabled": True}}}
    with pytest.raises(ConfigError):
        parse_set(["novalue"])


def test_merge_order(cfg):
    # site job_defaults < job file < --set
    job, site = resolve_job(cfg, "perlmutter_example_job.yaml", overrides=["resources.n_node=3"])
    assert site.name == "NERSC_Perlmutter"
    assert job.resources["n_node"] == 3
    assert job.resources["project"] == "m2616"
    assert job.duration == 1800


def test_site_defaults_used_when_job_omits(cfg, work):
    (work.jobs_dir / "min.yaml").write_text("site: ALCF_CRUX\ntemplate_file: crux_iri_submit_template.sh\n")
    job, _ = resolve_job(cfg, "min.yaml")
    assert job.resources["project"] == "ATLAS_workflow_ALCF"  # from job_defaults
    assert job.resources["custom_attributes"] == {"filesystems": "home:eagle"}


def test_cli_site_overrides_job_site(cfg):
    job, site = resolve_job(cfg, "perlmutter_example_job.yaml", site_name="ALCF_CRUX")
    assert site.name == "ALCF_CRUX" and job.site == "ALCF_CRUX"


def test_e2e_files_without_enabled_rejected(cfg):
    with pytest.raises(ConfigError, match="enabled is false"):
        resolve_job(cfg, "perlmutter_example_job.yaml", overrides=["staging.e2e.stagein.rucio=[a:b]"])


def test_required_kinds(cfg):
    job, nersc = resolve_job(cfg, "perlmutter_example_job.yaml")
    assert set(job.required_kinds(nersc)) == {"iri", "https"}  # https: stdout/stderr download
    job, _ = resolve_job(cfg, "perlmutter_example_job.yaml", overrides=["staging.https_stageout.stdout_stderr=false"])
    assert set(job.required_kinds(nersc)) == {"iri"}  # NERSC uploads via iri
    job, crux = resolve_job(cfg, "crux_example_job.yaml", overrides=["staging.https_stageout.stdout_stderr=false"])
    assert set(job.required_kinds(crux)) == {"iri", "https"}  # Crux uploads via globus_https
    job, _ = resolve_job(cfg, "crux_example_job.yaml", overrides=["staging.e2e.enabled=true"])
    assert "e2e" in job.required_kinds(crux)


def test_job_spec_crux_drops_unsupported_fields(cfg, tmp_path):
    job, site = resolve_job(cfg, "crux_example_job.yaml")
    sub, _ = jobs.prepare(cfg, job, site, "r1", tmp_path)
    res = sub.job_spec["resources"]
    assert "process_count" not in res and "processes_per_node" not in res and "cpu_cores_per_process" not in res
    assert res["node_count"] == 1
    assert sub.job_spec["attributes"]["custom_attributes"] == {"filesystems": "home:eagle", "place": "scatter"}
    assert sub.job_spec["directory"] == "/lus/eagle/projects/ATLAS_workflow_ALCF/usatlas/IRI_workdir/jobs/r1"


def test_job_spec_perlmutter(cfg, tmp_path):
    job, site = resolve_job(cfg, "perlmutter_example_job.yaml")
    sub, _ = jobs.prepare(cfg, job, site, "r2", tmp_path)
    spec = sub.job_spec
    assert spec["resources"]["process_count"] == 4
    assert spec["resources"]["cpu_cores_per_process"] == 64
    assert "gpu_cores_per_process" not in spec["resources"]
    assert spec["attributes"] == {"duration": 1800, "queue_name": "debug", "account": "m2616",
                                  "custom_attributes": {"constraint": "cpu"}}
    assert spec["stdout_path"] == "/global/cfs/cdirs/m2616/www/PanDA_logs/IRI/r2/r2_stdout.txt"
    assert (tmp_path / "job.yaml").is_file()


def test_template_rendering_keeps_bash_vars(cfg, tmp_path, work):
    (work.inputs_dir / "panda_token").write_text("t")
    job, site = resolve_job(cfg, "perlmutter_example_job.yaml", overrides=["inputs.x509UserProxy=null"])
    sub, local = jobs.prepare(cfg, job, site, "r3", tmp_path)
    text = (tmp_path / "executable_batch").read_text()
    assert "${input_archive}" in text  # bash variable untouched
    assert "#SBATCH --nodes=1" in text
    assert 'if [ -n "pandaTokenFilename" ]' in text  # uploaded input -> its remote name
    assert 'if [ -n "" ] && [ -f "" ]; then\n    export X509' in text  # missing input -> ""
    assert set(local) == {"pandaTokenFilename"}


@pytest.mark.parametrize("state,exit_code,expected", [
    ("new", None, jobs.SUBMITTED), ("queued", None, jobs.SUBMITTED), ("active", None, jobs.RUNNING),
    ("completed", 0, jobs.FINISHED), ("completed", None, jobs.FINISHED), ("completed", 1, jobs.FAILED),
    ("canceled", None, jobs.CANCELLED), ("failed", None, jobs.FAILED), ("weird", None, jobs.FAILED),
])
def test_map_state(state, exit_code, expected):
    assert jobs.map_state({"status": {"state": state, "exit_code": exit_code}}) == expected


def test_stdio_from_admincomment():
    job = {"status": {"meta_data": {"admincomment": '{"stdoutPath": "/a/o", "stderrPath": "/a/e"}'}}}
    assert jobs.stdio_paths_from_job(job) == ("/a/o", "/a/e")
    assert jobs.stdio_paths_from_job({}) == (None, None)


def test_rucio_path_matches_harvester():
    import hashlib

    scope, name = "mc23_13p6TeV", "EVNT.123._000001.pool.root.1"
    h = hashlib.md5(f"{scope}:{name}".encode()).hexdigest()
    assert rucio_path("/base", scope, name) == f"/base/mc23_13p6TeV/{h[0:2]}/{h[2:4]}/{name}"
    assert rucio_path("/base", "user.wguan", "f").startswith("/base/user/wguan/")


def test_e2e_plan_maps_only_hpc_side(cfg):
    site = cfg.site("ALCF_CRUX")
    p = plan(cfg, site, "stagein", rucio=["mc23:f1"])
    (src, dst), = p.items
    assert p.src.label == "BNL_dCache" and p.dst.label == "Crux"
    assert src.startswith("/pnfs/usatlas.bnl.gov/atlasdatadisk/rucio/mc23/")
    assert dst.startswith("/ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio/mc23/")  # mapped
    out = plan(cfg, site, "stageout", pairs=[("/lus/eagle/projects/ATLAS_workflow_ALCF/o.root", "/pnfs/x/o.root")])
    assert out.items == [("/ATLAS_workflow_ALCF/o.root", "/pnfs/x/o.root")]


def test_e2e_plan_manifest(cfg, tmp_path):
    m = tmp_path / "m.txt"
    m.write_text("# comment\n/pnfs/a /global/cfs/cdirs/m2616/a\n\n")
    p = plan(cfg, cfg.site("NERSC_Perlmutter"), "stagein", manifest=m)
    assert p.items == [("/pnfs/a", "/global/cfs/cdirs/m2616/a")]
    m.write_text("only-one-column\n")
    with pytest.raises(ConfigError):
        plan(cfg, cfg.site("NERSC_Perlmutter"), "stagein", manifest=m)


def test_check_under_refuses_outside_paths(cfg):
    site = cfg.site("NERSC_Perlmutter")
    roots = cleanup.site_roots(site)
    inside = site.remote_run_dir("R1")
    assert cleanup.check_under(inside + "/", roots) == inside
    for bad in ("/", "relative/x", site.work_dir, site.work_dir + "/../etc", "/tmp/x"):
        with pytest.raises(ConfigError):
            cleanup.check_under(bad, roots)
    assert cleanup.check_under("/tmp/x", roots, force=True) == "/tmp/x"
    with pytest.raises(ConfigError):
        cleanup.check_under("/", roots, force=True)


class RmIri:
    def __init__(self, rm_error=None, stat_error=None):
        self.rm_error, self.stat_error, self.calls = rm_error, stat_error, []

    def rm(self, path, **kw):
        self.calls.append(("rm", path, kw.get("resource_id")))
        if self.rm_error:
            raise self.rm_error

    def stat(self, path, **kw):
        if self.stat_error:
            raise self.stat_error
        return {}


def test_cleanup_run(cfg):
    site = cfg.site("ALCF_CRUX")
    iri = RmIri()
    assert cleanup.cleanup_run(iri, site, "R1") == {site.remote_run_dir("R1"): "removed"}
    with pytest.raises(ConfigError):
        cleanup.cleanup_run(iri, site, "../x")


def test_iri_rm_missing_is_ok_but_other_errors_raise():
    gone = RmIri(rm_error=RemoteError("task failed", status=None), stat_error=RemoteError("nf", status=404))
    assert cleanup.iri_rm(gone, "/a/b") is False
    broken = RmIri(rm_error=RemoteError("denied", status=500))
    with pytest.raises(RemoteError):
        cleanup.iri_rm(broken, "/a/b")


def test_e2e_cleanup_plan(cfg, tmp_path):
    site = cfg.site("ALCF_CRUX")
    m = tmp_path / "in.txt"
    m.write_text("/pnfs/a /lus/eagle/projects/ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio/a\n")
    p = cleanup_plan(site, manifest=m, rucio=["mc23:f1"])
    assert p.endpoint.label == "Crux"
    assert p.items[0] == "/ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio/a"  # mapped to Globus path
    assert p.items[1].startswith("/ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio/mc23/")
    with pytest.raises(ConfigError, match="--force"):
        cleanup_plan(site, paths=["/home/someone/file"])
    assert cleanup_plan(site, paths=["/lus/eagle/other/f"], force=True).items == ["/lus/eagle/other/f"]
    with pytest.raises(ConfigError):
        cleanup_plan(site)
