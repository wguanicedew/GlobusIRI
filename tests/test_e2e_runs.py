#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Tracked E2E transfers: `globusiri e2e stagein/stageout` create transfer runs that
monitor/daemon/runs/cancel/--resume handle like job runs."""

import pytest

from globusiri import workflow
from globusiri.cli import build_parser, cmd_e2e_stage
from globusiri.errors import ConfigError

from .test_async_workflow import env  # noqa: F401  (fixture: fake IRI, HTTPS and Globus)

SDCC_RUCIO = "/dcint-door003/usatlas.bnl.gov/atlasdatadisk/rucio"
PM_SRC = "/global/cfs/cdirs/m2616/harvester_workdir/pilot_env/perlmutter_iri_main.sh"
FILE_A = SDCC_RUCIO + "/user/wguan/cb/6a/fileA"


def test_submit_many_then_monitor(env):  # noqa: F811
    g, iri, gl = env
    a = g.e2e_submit("NERSC_Perlmutter", "stagein", [("/pnfs/x/a.root", "/global/cfs/cdirs/m2616/a.root")],
                     run_id="T1", validate=False)
    b = g.e2e_submit("ALCF_CRUX", "stagein", rucio=["user.wguan:fileA"], run_id="T2", validate=False)
    c = g.e2e_submit("NERSC_Perlmutter", "stageout", [(PM_SRC, SDCC_RUCIO, "user.wguan", "fileA")],
                     run_id="T3", validate=False)
    for r in (a, b, c):
        assert r["status"] == "active" and r["kind"] == "transfer" and r["plugin_status"] == "running"
    assert steps(a) == {"validate": "skipped", "e2e_stagein": "running", "e2e_stageout": "pending"}
    assert steps(c) == {"validate": "skipped", "e2e_stagein": "skipped", "e2e_stageout": "running"}
    assert [s[0] for s in gl.submitted] == ["stagein", "stagein", "stageout"]
    assert gl.submitted[2][2] == [(PM_SRC, FILE_A)]
    assert not iri.calls  # no IRI at all

    # one monitor pass checks every transfer; nothing done yet
    assert set(g.monitor()) == {"T1", "T2", "T3"}
    gl.tasks["T1"] = "SUCCEEDED"
    gl.tasks["T3"] = "SUCCEEDED"
    res = g.monitor()
    assert res["T1"]["status"] == "finished" and res["T3"]["status"] == "finished"
    assert res["T2"]["status"] == "active"
    gl.tasks["T2"] = "FAILED"
    r = g.monitor()["T2"]
    assert r["status"] == "failed" and "transfer failed" in r["message"]
    assert g.monitor() == {}  # nothing active left

    # retry the failed transfer: a new Globus task is submitted
    r = g.monitor(["T2"], resume=True)["T2"]
    assert r["status"] == "active" and len(gl.submitted) == 4
    assert {row["run_id"] for row in g.runs(summary=True)} >= {"T1", "T2", "T3"}

    g.cancel("T2")
    assert g.run_state("T2")["status"] == "cancelled" and gl.cancelled == ["T4"]


def test_endpoint_and_sync_level_reach_globus(env, monkeypatch):  # noqa: F811
    g, iri, gl = env
    seen = []
    orig = gl.submit

    def submit(transfer, config, p, *, label=None, **kw):
        seen.append((p.src.label, kw.get("sync_level"), label))
        return orig(transfer, config, p, label=label, **kw)

    monkeypatch.setattr(workflow.e2e_staging, "submit", submit)
    g.e2e_submit("NERSC_Perlmutter", "stagein", [("/a", "/global/cfs/cdirs/m2616/a")], endpoint="BNL_dCache",
                 sync_level="checksum", run_id="T5", validate=False)
    assert seen == [("BNL_dCache", "checksum", "globusiri-T5-IN")]


def test_validation_checks_only_the_e2e_token(env, monkeypatch):  # noqa: F811
    g, iri, gl = env
    seen = []

    def fake_validate(config, *, job=None, kinds=(), **kw):
        seen.append(list(kinds))
        return []

    monkeypatch.setattr(workflow, "validate", fake_validate)
    r = g.e2e_submit("NERSC_Perlmutter", "stagein", [("/a", "/global/cfs/cdirs/m2616/a")], run_id="T6")
    assert seen == [["e2e"]] and steps(r)["validate"] == "done"


def test_bad_entry_fails_before_the_run_exists(env, work):  # noqa: F811
    g, iri, gl = env
    with pytest.raises(ConfigError, match="needs a scope"):
        g.e2e_submit("NERSC_Perlmutter", "stagein", [{"src": "/a", "dst": "/b", "rucio_deterministic": True}],
                     run_id="T7", validate=False)
    assert not work.run_dir("T7").exists() and not gl.submitted


def test_wait_polls_until_done(env, monkeypatch):  # noqa: F811
    g, iri, gl = env

    def finish_on_poll(transfer, tid):
        gl.tasks[tid] = "SUCCEEDED"
        return gl.task_info(transfer, tid)

    monkeypatch.setattr(workflow.e2e_staging, "task_info", finish_on_poll)
    r = g.e2e_submit("NERSC_Perlmutter", "stagein", [("/a", "/global/cfs/cdirs/m2616/a")], run_id="T8",
                     validate=False, wait=True, interval=0)
    assert r["status"] == "finished"


def test_cli_no_wait(env, capsys):  # noqa: F811
    g, iri, gl = env
    args = build_parser().parse_args(["e2e", "stageout", "NERSC_Perlmutter", PM_SRC, SDCC_RUCIO,
                                      "--rucio-deterministic", "--scope", "user.wguan", "--name", "fileA",
                                      "--run-id", "T9", "--no-validate", "--no-wait"])
    assert cmd_e2e_stage(args, g) == 0
    out = capsys.readouterr().out
    assert "T9" in out and "next: globusiri monitor T9" in out
    assert gl.submitted[-1][2] == [(PM_SRC, FILE_A)]
    base = ["e2e", "stageout", "NERSC_Perlmutter", PM_SRC, SDCC_RUCIO, "--no-wait", "--no-validate"]
    for extra, msg in ((["--scope", "user.wguan"], "need --rucio-deterministic"),
                       (["--rucio-deterministic"], "needs --scope")):
        with pytest.raises(ConfigError, match=msg):
            cmd_e2e_stage(build_parser().parse_args(base + extra), g)


def steps(r):
    return {k: v["status"] for k, v in r["steps"].items()}
