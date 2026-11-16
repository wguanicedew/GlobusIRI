"""Rucio deterministic destinations for E2E transfers (offline: plans only, nothing is sent)."""

import pytest

from globusiri import services
from globusiri.api import GlobusIRI
from globusiri.cli import build_parser, cmd_e2e_stage
from globusiri.errors import ConfigError
from globusiri.jobfile import resolve_job
from globusiri.staging.e2e import cleanup_plan, deterministic_dst, plan, read_manifest, rucio_path
from globusiri.workflow import _e2e_sources, file_pairs

SDCC_RUCIO = "/dcint-door003/usatlas.bnl.gov/atlasdatadisk/rucio"
PM_SRC = "/global/cfs/cdirs/m2616/harvester_workdir/pilot_env/perlmutter_iri_main.sh"
FILE_A = SDCC_RUCIO + "/user/wguan/cb/6a/fileA"  # md5("user.wguan:fileA") = cb6a...


def test_deterministic_dst_rules():
    assert deterministic_dst("/a/x.root", "/b/y.root") == "/b/y.root"  # default: DST is the file
    assert deterministic_dst("/a/x.root", "/b/y.root", scope="mc23") == "/b/y.root"  # scope alone: no effect
    assert deterministic_dst("/a/x.root", "/b/rucio", deterministic=True, scope="mc23") == \
        rucio_path("/b/rucio", "mc23", "x.root")
    assert deterministic_dst(PM_SRC, SDCC_RUCIO + "/", deterministic=True, scope="user.wguan", name="fileA") == FILE_A
    with pytest.raises(ConfigError, match="needs a scope"):
        deterministic_dst("/a/x", "/b", deterministic=True)
    with pytest.raises(ConfigError, match="needs rucio_deterministic"):
        deterministic_dst("/a/x", "/b", scope="s", name="n")
    with pytest.raises(ConfigError, match="invalid Rucio name"):
        deterministic_dst("/a/x", "/b", deterministic=True, scope="s", name="a/b")


def test_chain_plans(cfg):
    """Perlmutter -> SDCC as user.wguan:fileA -> Perlmutter and Crux rucio dirs."""
    out = plan(cfg, cfg.site("NERSC_Perlmutter"), "stageout", pairs=[(PM_SRC, SDCC_RUCIO)],
               deterministic=True, scope="user.wguan")
    assert out.items == [(PM_SRC, rucio_path(SDCC_RUCIO, "user.wguan", "perlmutter_iri_main.sh"))]  # SRC's basename
    out = plan(cfg, cfg.site("NERSC_Perlmutter"), "stageout", pairs=[(PM_SRC, SDCC_RUCIO, "user.wguan", "fileA")])
    assert (out.src.label, out.dst.label) == ("NERSC", "BNL_dCache")
    assert out.items == [(PM_SRC, FILE_A)]

    pm = plan(cfg, cfg.site("NERSC_Perlmutter"), "stagein", rucio=["user.wguan:fileA"])
    assert pm.items == [(FILE_A, "/global/cfs/cdirs/m2616/harvester_workdir/rucio/user/wguan/cb/6a/fileA")]
    crux = plan(cfg, cfg.site("ALCF_CRUX"), "stagein", rucio=["user.wguan:fileA"])
    assert (crux.src.label, crux.dst.label) == ("BNL_dCache", "Crux")
    assert crux.items == [(FILE_A, "/ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio/user/wguan/cb/6a/fileA")]


def test_manifest(cfg, tmp_path):
    m = tmp_path / "m.txt"
    m.write_text("/pnfs/a/x.root /global/d/x.root\n"           # follows the defaults
                 "/pnfs/a/y.root /global/d mc23:renamed\n")    # third column: deterministic, own DID
    assert read_manifest(m) == [("/pnfs/a/x.root", "/global/d/x.root"),
                                ("/pnfs/a/y.root", rucio_path("/global/d", "mc23", "renamed"))]
    items = read_manifest(m, deterministic=True, scope="user.me")
    assert items[0][1] == rucio_path("/global/d/x.root", "user.me", "x.root")


def test_job_file_defaults_and_per_file_overrides():
    e2e = {"enabled": True, "scope": "user.wguan", "rucio_deterministic": True}
    spec = {"files": [
        {"src": "/pnfs/a/x.root", "dst": "/global/rucio"},                                  # e2e defaults
        {"src": "/pnfs/a/y.root", "dst": "/global/rucio", "name": "y2.root"},
        {"src": "/pnfs/a/z.root", "dst": "/global/rucio", "scope": "mc23"},
        {"src": "/pnfs/a/f.txt", "dst": "/global/run/f.txt", "rucio_deterministic": False},   # plain file
        "/pnfs/a/w.root /global/rucio data18:w2.root",
    ]}
    assert [d for _, d in file_pairs(spec, e2e)] == [
        rucio_path("/global/rucio", "user.wguan", "x.root"), rucio_path("/global/rucio", "user.wguan", "y2.root"),
        rucio_path("/global/rucio", "mc23", "z.root"), "/global/run/f.txt",
        rucio_path("/global/rucio", "data18", "w2.root")]
    # default off: scope alone changes nothing; one file can switch it on
    e2e = {"enabled": True, "scope": "user.wguan"}
    spec = {"files": [{"src": "/a/x", "dst": "/b/x"},
                      {"src": "/a/y", "dst": "/b/rucio", "rucio_deterministic": True}]}
    assert file_pairs(spec, e2e) == [("/a/x", "/b/x"), ("/a/y", rucio_path("/b/rucio", "user.wguan", "y"))]
    # a stagein/stageout section overrides staging.e2e
    spec = {"rucio_deterministic": True, "scope": "mc23", "files": ["/a/x /b/rucio"]}
    assert file_pairs(spec, {"rucio_deterministic": False}) == [("/a/x", rucio_path("/b/rucio", "mc23", "x"))]
    with pytest.raises(ConfigError, match="needs a scope"):
        file_pairs({"files": ["/a/x /b"]}, {"rucio_deterministic": True})
    with pytest.raises(ConfigError, match="true or false"):
        file_pairs({"files": ["/a/x /b"]}, {"rucio_deterministic": "yes"})


def test_job_file_layout_reaches_the_plan(cfg, tmp_path):
    job, site = resolve_job(cfg, "perlmutter_transfer_job.yaml", overrides={"staging": {"e2e": {
        "scope": "user.wguan", "rucio_deterministic": True,
        "stagein": {"files": [{"src": FILE_A.replace("/user/wguan/cb/6a/fileA", "/x/fileA"),
                               "dst": "/global/cfs/cdirs/m2616/harvester_workdir/rucio", "name": "fileA"}]},
        "stageout": {"files": [{"src": "/global/cfs/cdirs/m2616/harvester_workdir/out.txt",
                                "dst": "/pnfs/usatlas.bnl.gov/out.txt", "rucio_deterministic": False}]}}}})
    manifest, rucio, pairs, det, scope = _e2e_sources(cfg, job.e2e, "stagein")
    p = plan(cfg, site, "stagein", manifest=manifest, rucio=rucio, pairs=pairs, deterministic=det, scope=scope)
    assert p.items[0][1] == "/global/cfs/cdirs/m2616/harvester_workdir/rucio/user/wguan/cb/6a/fileA"
    manifest, rucio, entries, det, scope = _e2e_sources(cfg, job.e2e, "stageout")
    p = plan(cfg, site, "stageout", pairs=entries, deterministic=det, scope=scope)
    assert p.items == [("/global/cfs/cdirs/m2616/harvester_workdir/out.txt", "/dcint-door003/usatlas.bnl.gov/out.txt")]


def test_cli(cfg, monkeypatch):
    class FakeTransfer:
        def call(self, what, fn):
            return {"task_id": "T1"}

    monkeypatch.setattr(services, "transfer", lambda c: FakeTransfer())
    g = GlobusIRI(cfg)
    seen = {}
    real_plan = g.e2e_plan

    def spy(*a, **kw):
        seen["plan"] = real_plan(*a, **kw)
        return seen["plan"]

    monkeypatch.setattr(g, "e2e_plan", spy)
    base = ["e2e", "stageout", "NERSC_Perlmutter", PM_SRC, SDCC_RUCIO]
    assert cmd_e2e_stage(build_parser().parse_args(
        base + ["--rucio-deterministic", "--scope", "user.wguan", "--name", "fileA", "--no-wait"]), g) == 0
    assert seen["plan"].items == [(PM_SRC, FILE_A)]
    assert cmd_e2e_stage(build_parser().parse_args(base + ["--no-wait"]), g) == 0
    assert seen["plan"].items == [(PM_SRC, SDCC_RUCIO)]  # default: DST is the file
    for extra, msg in ((["--scope", "user.wguan"], "need --rucio-deterministic"),
                       (["--rucio-deterministic"], "needs --scope")):
        with pytest.raises(ConfigError, match=msg):
            cmd_e2e_stage(build_parser().parse_args(base + extra), g)


def test_cleanup_follows_deterministic_stagein(cfg, tmp_path):
    m = tmp_path / "in.txt"
    m.write_text(f"{FILE_A} /global/cfs/cdirs/m2616/harvester_workdir/rucio\n")
    p = cleanup_plan(cfg.site("NERSC_Perlmutter"), manifest=m, deterministic=True, scope="user.wguan")
    assert p.items == ["/global/cfs/cdirs/m2616/harvester_workdir/rucio/user/wguan/cb/6a/fileA"]
