"""Index recovery and actual concurrent processes sharing a WORK_DIR."""

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from globusiri.runindex import RunIndex
from globusiri.secure_io import write_json_private


def test_import_and_rebuild(tmp_path, monkeypatch):
    root = tmp_path / "runs"
    data = {"run_id": "old", "status": "active", "steps": {}}
    write_json_private(root / "old" / "state.json", data)
    write_json_private(root / "bad" / "state.json", [])
    index = RunIndex(root)
    assert index.query(status="active")[0]["run_id"] == "old"
    assert index.path.stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(index.path) as con:
        assert con.execute("PRAGMA journal_mode").fetchone()[0] == "wal"

    data["status"] = "finished"
    write_json_private(root / "old" / "state.json", data)
    # Simulates a process killed after JSON replacement, before index commit.
    assert index.get("old")["status"] == "active"
    assert index.rebuild() == 1
    assert index.get("old")["status"] == "finished"

    def no_scan(*args):
        pytest.fail("normal queries must not scan run directories")

    monkeypatch.setattr(index, "_scan_into", no_scan)
    assert index.query(status="active") == []


def test_rebuild_rollback_preserves_index(tmp_path, monkeypatch):
    index = RunIndex(tmp_path / "runs")
    index.save({"run_id": "one", "status": "active"})

    def fail(con):
        raise RuntimeError("interrupted import")

    monkeypatch.setattr(index, "_scan_into", fail)
    with pytest.raises(RuntimeError, match="interrupted import"):
        index.rebuild()
    assert index.get("one")["status"] == "active"


def test_concurrent_submit_monitor_and_rebuild(work):
    # Each child runs the real workflow with a fake HPC backend. Two submitters,
    # two monitors and a rebuilder start together against an uninitialized index.
    script = r'''
import sys
import time
from pathlib import Path
from globusiri import GlobusIRI, services
from globusiri.runindex import RunIndex
from tests.test_validate_workflow import FakeHttps, FakeIri

root, role = Path(sys.argv[1]), sys.argv[2]
g = GlobusIRI(work_dir=root)
iri = FakeIri(["completed"])
services.iri_client = lambda *a, **kw: iri
services.https_client = lambda *a, **kw: FakeHttps(200)
original_launch = iri.launch_job
def launch(*a, **kw):
    with open(root / "launches.txt", "a") as f:
        f.write("launch\n")
    time.sleep(0.005)
    return original_launch(*a, **kw)
iri.launch_job = launch
while not (root / "start").exists():
    time.sleep(0.005)
if role.startswith("submit"):
    for i in range(8):
        g.submit("perlmutter_hello_job.yaml", run_id=f"{role}-{i}",
                 validate=False, set=["staging.https_stageout={stdout_stderr: false, files: []}"])
elif role == "rebuild":
    for i in range(12):
        RunIndex(root / "runs").rebuild()
        time.sleep(0.005)
else:
    for i in range(24):
        result = g.monitor()
        assert all(not isinstance(v, str) or v.startswith("busy") for v in result.values()), result
        time.sleep(0.005)
'''
    children = [subprocess.Popen([sys.executable, "-c", script, str(work.root), role],
                                 cwd=Path(__file__).resolve().parents[1],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                for role in ("submit1", "submit2", "monitor1", "monitor2", "rebuild")]
    (work.root / "start").touch()
    try:
        for child in children:
            out, err = child.communicate(timeout=30)
            assert child.returncode == 0, out + err
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.communicate()
    index = RunIndex(work.runs_dir)
    assert index.counts() == {"finished": 16}
    assert len((work.root / "launches.txt").read_text().splitlines()) == 16
    for row in index.query():
        data = json.loads((work.run_dir(row["run_id"]) / "state.json").read_text())
        assert data["status"] == row["status"] == "finished"
    with sqlite3.connect(index.path) as con:
        assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_cli_index_listing_and_reindex(cfg, capsys, monkeypatch):
    from globusiri import workflow
    from globusiri.cli import build_parser, cmd_runs
    from globusiri.api import GlobusIRI

    g = GlobusIRI(cfg)
    g.submit("perlmutter_hello_job.yaml", run_id="cli-run", advance=False)
    original = workflow.RunState.load

    def no_load(*args):
        pytest.fail("tabular listing should read only the index")

    monkeypatch.setattr(workflow.RunState, "load", no_load)
    args = build_parser().parse_args(["runs", "--status", "active"])
    assert cmd_runs(args, g) == 0
    assert "cli-run" in capsys.readouterr().out
    monkeypatch.setattr(workflow.RunState, "load", original)
    args = build_parser().parse_args(["runs", "--reindex", "--json"])
    assert cmd_runs(args, g) == 0
    rows = json.loads(capsys.readouterr().out)
    assert rows[0]["run_id"] == "cli-run" and "steps" in rows[0]
