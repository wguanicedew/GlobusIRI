import subprocess
from pathlib import Path

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
    assert set(job.required_kinds(nersc)) == {"iri", "https"}  # NERSC uploads via globus_https
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


def test_inputs_packed_into_one_archive(cfg, tmp_path, work):
    import tarfile

    (work.inputs_dir / "panda_token").write_text("t")
    (work.inputs_dir / "panda_token_key").write_text("k")
    job, site = resolve_job(cfg, "crux_example_job.yaml", overrides=["inputs.x509UserProxy=null"])
    sub, _ = jobs.prepare(cfg, job, site, "r4", tmp_path)
    assert sub.archive == str(tmp_path / jobs.INPUT_ARCHIVE)
    assert sorted(sub.archived) == ["pandaTokenFilename", "pandaTokenKeyPath"]
    with tarfile.open(sub.archive) as tar:
        assert sorted(tar.getnames()) == ["pandaTokenFilename", "pandaTokenKeyPath"]
        assert tar.extractfile("pandaTokenFilename").read() == b"t"
    text = (tmp_path / "executable_batch").read_text()
    # the Crux template extracts the archive itself, before it looks at the input files
    assert text.count("input stage-in: rank 0") == 1
    assert text.index("cd /lus/eagle") < text.index('GLOBUSIRI_ARCHIVE="globusiri_inputs.tar.gz"')
    assert text.index("GLOBUSIRI_ARCHIVE=") < text.index('if [ -n "pandaTokenFilename" ]')


def test_stagein_inserted_into_template_without_it(cfg, tmp_path, work):
    # perlmutter_iri_submit_template.sh (the Harvester pilot template) has no {inputArchive}
    (work.inputs_dir / "panda_token").write_text("t")
    job, site = resolve_job(cfg, "perlmutter_example_job.yaml", overrides=["inputs.x509UserProxy=null"])
    jobs.prepare(cfg, job, site, "r8", tmp_path)
    text = (tmp_path / "executable_batch").read_text()
    assert text.count("input stage-in: rank 0") == 1
    assert text.index("#SBATCH -e") < text.index("GLOBUSIRI_ARCHIVE=") < text.index("export TZ=UTC0")


@pytest.mark.parametrize("template", ["shell_job_template.sh", "crux_iri_submit_template.sh"])
def test_templates_carry_the_stagein_snippet(template):
    from globusiri.paths import package_data_dir

    assert jobs.STAGEIN_SNIPPET in (package_data_dir() / "templates" / template).read_text()


def test_no_inputs_no_archive(cfg, tmp_path):
    job, site = resolve_job(cfg, "perlmutter_hello_job.yaml")
    sub, _ = jobs.prepare(cfg, job, site, "r5", tmp_path)
    assert sub.archive is None and not (tmp_path / jobs.INPUT_ARCHIVE).exists()
    assert 'GLOBUSIRI_ARCHIVE=""' in (tmp_path / "executable_batch").read_text()  # block is a no-op


@pytest.mark.parametrize("name", ["/abs", "../x", "a//b", "executable_batch", "globusiri_inputs.tar.gz"])
def test_bad_input_names_rejected(cfg, tmp_path, name):
    job, site = resolve_job(cfg, "perlmutter_hello_job.yaml", overrides=[f"inputs={{'{name}': x}}"])
    with pytest.raises(ConfigError, match="not a valid remote file name"):
        jobs.prepare(cfg, job, site, "r6", tmp_path)


def test_stagein_rank0_extracts_others_wait(cfg, tmp_path, work):
    import os
    import time

    (work.inputs_dir / "words.txt").write_text("one two three\n")
    job, site = resolve_job(cfg, "perlmutter_wordcount_job.yaml")
    local_dir = tmp_path / "local"
    sub, _ = jobs.prepare(cfg, job, site, "r7", local_dir)
    remote = tmp_path / "remote"  # stands in for the shared remote run dir
    remote.mkdir()
    (remote / jobs.INPUT_ARCHIVE).write_bytes(Path(sub.archive).read_bytes())
    script = (local_dir / "executable_batch").read_text().replace(sub.remote_dir, str(remote))
    (remote / "job.sh").write_text(script)

    def start(rank):
        env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "PMI", "PALS_", "OMPI_"))}
        env["PMI_RANK"] = str(rank)
        return subprocess.Popen(["/bin/sh", str(remote / "job.sh")], env=env, cwd=tmp_path,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    waiters = [start(1), start(2)]
    time.sleep(0.5)
    assert all(w.poll() is None for w in waiters)  # blocked until rank 0 has extracted
    assert not (remote / "words.txt").exists()
    rank0 = start(0)
    for p in [rank0, *waiters]:
        out, err = p.communicate(timeout=30)
        assert p.returncode == 0, out + err
    assert (remote / "wordcount.txt").read_text().strip() == "3"
    assert (remote / jobs.STAGEIN_DONE).exists()


def test_stagein_other_ranks_fail_fast_when_rank0_fails(cfg, tmp_path):
    work_dir = tmp_path / "remote"
    work_dir.mkdir()
    (work_dir / jobs.INPUT_ARCHIVE).write_text("not a tarball")
    script = work_dir / "job.sh"
    script.write_text(jobs.stagein_block(str(work_dir)) + "echo ran\n")
    env = {"PATH": "/usr/bin:/bin", "PMI_RANK": "0"}
    r0 = subprocess.run(["/bin/sh", str(script)], env=env, capture_output=True, text=True)
    assert r0.returncode != 0 and (work_dir / jobs.STAGEIN_FAILED).exists()
    r1 = subprocess.run(["/bin/sh", str(script)], env={**env, "PMI_RANK": "1"}, capture_output=True, text=True,
                        timeout=10)
    assert r1.returncode != 0 and "rank 0 failed" in r1.stderr and "ran" not in r1.stdout

@pytest.mark.parametrize("job_name,output,expected", [
    ("perlmutter_hello_job.yaml", "result.txt", "Job completed"),
    ("crux_hello_job.yaml", "result.txt", "Job completed"),
    ("perlmutter_wordcount_job.yaml", "wordcount.txt", "3"),
    ("perlmutter_transfer_job.yaml", "wordcount.txt", "3"),
])
def test_shell_examples_produce_documented_outputs(cfg, tmp_path, job_name, output, expected):
    job, site = resolve_job(cfg, job_name)
    ph = jobs.placeholders(job, site, "example", {})
    ph["work_dir"] = str(tmp_path)
    script = jobs.render_template(cfg.paths.find_template(job.template_file), ph, tmp_path / "job.sh")
    (tmp_path / "words.txt").write_text("one two three\n")
    (tmp_path / "input.txt").write_text("one two three\n")
    subprocess.run(["/bin/bash", str(script)], check=True, capture_output=True)
    assert (tmp_path / output).read_text().strip() == expected


@pytest.mark.parametrize("n_node", [1, 2])
def test_perlmutter_mpi_example_runs_once_per_rank(cfg, tmp_path, n_node):
    """NERSC's IRI runs the script under srun: one copy per rank, run concurrently here."""
    import os

    job, site = resolve_job(cfg, "perlmutter_mpi_job.yaml", overrides=[f"resources.n_node={n_node}"])
    ph = jobs.placeholders(job, site, "example", {})
    ph["work_dir"] = str(tmp_path)
    script = jobs.render_template(cfg.paths.find_template(job.template_file), ph, tmp_path / "job.sh")
    assert "srun" not in job.template_vars["command"].replace("srun with", "")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "PMI", "PALS_", "OMPI_"))}
    nproc = 4 * n_node
    ranks = [subprocess.Popen(["/bin/bash", str(script)], env={**env, "SLURM_PROCID": str(r), "SLURM_NTASKS": str(nproc)},
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
             for r in reversed(range(nproc))]
    for p in ranks:
        out, err = p.communicate(timeout=60)
        assert p.returncode == 0, out + err
    lines = (tmp_path / "ranks.txt").read_text().splitlines()
    assert [line.split()[1] for line in lines] == [str(r) for r in range(nproc)]


FAKE_MPIEXEC = """#!/bin/bash
# fake PALS mpiexec: run the trailing `bash -c ...` once per rank
while [ $# -gt 0 ] && [ "$1" != bash ]; do shift; done
for r in $(seq 0 $((GLOBUSIRI_NPROC - 1))); do PALS_RANKID=$r PALS_LOCAL_RANKID=$r "$@"; done
"""


@pytest.mark.parametrize("n_node", [1, 2])
def test_crux_mpi_example_starts_ranks_with_mpiexec(cfg, tmp_path, n_node):
    """Crux's IRI drops the per-process fields: the script runs once and mpiexec starts the ranks."""
    import os

    job, site = resolve_job(cfg, "crux_mpi_job.yaml", overrides=[f"resources.n_node={n_node}"])
    ph = jobs.placeholders(job, site, "example", {})
    ph["work_dir"] = str(tmp_path)
    script = jobs.render_template(cfg.paths.find_template(job.template_file), ph, tmp_path / "job.sh")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "mpiexec").write_text(FAKE_MPIEXEC)
    (bin_dir / "mpiexec").chmod(0o755)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "PMI", "PALS_", "OMPI_"))}
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    subprocess.run(["/bin/bash", str(script)], check=True, capture_output=True, env=env)
    lines = (tmp_path / "ranks.txt").read_text().splitlines()
    assert [line.split()[1] for line in lines] == [str(r) for r in range(4 * n_node)]


def test_mpi_examples_job_spec(cfg, tmp_path):
    job, site = resolve_job(cfg, "perlmutter_mpi_job.yaml")
    spec = jobs.prepare(cfg, job, site, "m1", tmp_path)[0].job_spec
    assert spec["resources"]["process_count"] == 4 and spec["resources"]["processes_per_node"] == 4
    assert spec["resources"]["cpu_cores_per_process"] == 64 and "launcher" not in spec
    job, site = resolve_job(cfg, "crux_mpi_job.yaml")
    spec = jobs.prepare(cfg, job, site, "m2", tmp_path)[0].job_spec
    assert spec["resources"] == {"node_count": 1, "exclusive_node_use": True}  # PBS: nodes only
    assert 'export GLOBUSIRI_NPROC_PER_NODE=4' in (tmp_path / "executable_batch").read_text()

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
    assert src.startswith("/dcint-door003/usatlas.bnl.gov/atlasdatadisk/rucio/mc23/")
    assert dst.startswith("/ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio/mc23/")  # mapped
    out = plan(cfg, site, "stageout", pairs=[("/lus/eagle/projects/ATLAS_workflow_ALCF/o.root", "/pnfs/x/o.root")])
    assert out.items == [("/ATLAS_workflow_ALCF/o.root", "/dcint-door003/x/o.root")]  # BNL path_map


def test_e2e_plan_manifest(cfg, tmp_path):
    m = tmp_path / "m.txt"
    m.write_text("# comment\n/pnfs/a /global/cfs/cdirs/m2616/a\n\n")
    p = plan(cfg, cfg.site("NERSC_Perlmutter"), "stagein", manifest=m)
    assert p.items == [("/dcint-door003/a", "/global/cfs/cdirs/m2616/a")]  # /pnfs mapped by BNL path_map
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


# ---------------------------------------------------------------------- script_file
def test_script_file_used_verbatim(cfg, tmp_path, work, monkeypatch):
    monkeypatch.chdir(tmp_path)
    job, site = resolve_job(cfg, "perlmutter_script_job.yaml")
    # found next to the job file, stored absolute so a resumed run finds it from the snapshot
    assert job.script_file == str((work.jobs_dir / "perlmutter_script_job.sh").resolve())
    assert job.template_file is None
    sub, _ = jobs.prepare(cfg, job, site, "s1", tmp_path / "run")
    assert (tmp_path / "run" / "executable_batch").read_text() == Path(job.script_file).read_text()
    assert sub.job_spec["resources"]["process_count"] == 4
    resumed, _ = resolve_job(cfg, snapshot_data=job.data)
    assert resumed.script_file == job.script_file


def test_script_file_no_rendering_and_stagein(cfg, tmp_path, work, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "mine.sh").write_text("#!/bin/bash\n#SBATCH -q debug\necho {work_dir} ${HOME}\n")
    (work.inputs_dir / "words.txt").write_text("a b\n")
    job, site = resolve_job(cfg, "perlmutter_hello_job.yaml",
                            overrides=["script_file=mine.sh", "inputs={words.txt: inputs/words.txt}"])
    sub, _ = jobs.prepare(cfg, job, site, "s2", tmp_path / "run")
    text = (tmp_path / "run" / "executable_batch").read_text()
    assert "echo {work_dir} ${HOME}" in text  # not rendered
    assert text.index("#SBATCH -q debug") < text.index("GLOBUSIRI_ARCHIVE=") < text.index("echo {work_dir}")
    assert sub.archived == ["words.txt"]


def test_script_file_that_extracts_itself_gets_no_block(cfg, tmp_path, work, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "mine.sh").write_text("#!/bin/bash\ntar -xzf globusiri_inputs.tar.gz\n")
    (work.inputs_dir / "words.txt").write_text("a b\n")
    job, site = resolve_job(cfg, "perlmutter_hello_job.yaml",
                            overrides=["script_file=mine.sh", "inputs={words.txt: inputs/words.txt}"])
    jobs.prepare(cfg, job, site, "s3", tmp_path / "run")
    assert "GLOBUSIRI_ARCHIVE=" not in (tmp_path / "run" / "executable_batch").read_text()


def test_script_file_errors(cfg, tmp_path, work, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError, match="not both"):
        resolve_job(cfg, "perlmutter_script_job.yaml",
                    overrides=["template_file=shell_job_template.sh", "script_file=x.sh"])
    # a --set of the other kind replaces the job file's choice
    job, _ = resolve_job(cfg, "perlmutter_script_job.yaml", overrides=["template_file=shell_job_template.sh"])
    assert job.template_file == "shell_job_template.sh" and job.script_file is None
    with pytest.raises(ConfigError, match="script_file 'nope.sh' not found"):
        resolve_job(cfg, "perlmutter_hello_job.yaml", overrides=["script_file=nope.sh"])
    (tmp_path / "noshebang.sh").write_text("echo hi\n")
    job, site = resolve_job(cfg, "perlmutter_script_job.yaml", overrides=["script_file=noshebang.sh"])
    with pytest.raises(ConfigError, match="must start with a #! line"):
        jobs.prepare(cfg, job, site, "s4", tmp_path / "run")


def test_script_file_ignores_site_default_template(cfg, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg.site("NERSC_Perlmutter").job_defaults["template_file"] = "shell_job_template.sh"
    job, _ = resolve_job(cfg, "perlmutter_script_job.yaml")
    assert job.template_file is None and job.script_file


def test_script_example_runs(cfg, tmp_path, monkeypatch):
    import os

    monkeypatch.chdir(tmp_path)
    job, site = resolve_job(cfg, "perlmutter_script_job.yaml")
    assert "\nsrun" not in Path(job.script_file).read_text()
    env = {k: v for k, v in os.environ.items() if not k.startswith("SLURM_")}
    for rank in (3, 2, 1, 0):  # one copy per rank, as NERSC's IRI starts it
        r = subprocess.run(["/bin/bash", job.script_file], check=True, capture_output=True, text=True,
                           env={**env, "SLURM_PROCID": str(rank), "SLURM_NTASKS": "4"}, cwd=tmp_path)
        assert r.stdout.startswith(f"rank {rank} of 4 on ")
    assert (tmp_path / "result.txt").read_text().strip() == "Job completed"
