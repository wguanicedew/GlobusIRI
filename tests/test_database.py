#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Run index database: configuration, the SQLite single-writer lock, namespaces, MySQL SQL."""

import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from globusiri.config import Config
from globusiri.errors import ConfigError
from globusiri.orm import ENV_URL, DatabaseConfig
from globusiri.orm.models import Run
from sqlalchemy.dialects import sqlite
from globusiri.orm.session import get_engine, write_lock
from globusiri.runindex import RunIndex, _upsert

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def no_env_url(monkeypatch):
    monkeypatch.delenv(ENV_URL, raising=False)


def _config(work, database=None):
    raw = yaml.safe_load(work.default_config.read_text())
    if database is not None:
        raw["database"] = database
    return Config(raw, path=work.default_config, paths=work)


def test_default_is_sqlite_under_work_dir(work, no_env_url):
    cfg = _config(work)
    assert cfg.database.backend == "sqlite"
    assert cfg.database.sqlite_path == (work.runs_dir / "index.db").resolve()
    assert cfg.database.namespace == "default"
    index = cfg.run_index()
    index.save({"run_id": "r1", "status": "active"})
    assert index.get("r1")["status"] == "active"
    assert index.path.stat().st_mode & 0o777 == 0o600


def test_mysql_url_namespace_and_hidden_password(work, monkeypatch, no_env_url):
    monkeypatch.setenv("DBPW", "s3cret")
    cfg = _config(work, {"url": "mysql+pymysql://harvester:${DBPW}@dbhost:3306/harvester", "pool_recycle": 600})
    db = cfg.database
    assert db.backend == "mysql" and db.sqlite_path is None
    assert db.namespace == str(work.runs_dir.resolve())
    assert db.options == {"pool_recycle": 600}
    assert "s3cret" not in db.display and "dbhost:3306/harvester" in db.display
    assert _config(work, {"url": "mariadb+pymysql://u:p@h/db", "namespace": "prod"}).database.namespace == "prod"


def test_env_url_overrides_config(work, monkeypatch):
    monkeypatch.setenv(ENV_URL, "mysql+pymysql://u:p@envhost/db")
    assert _config(work, {"url": "mysql+pymysql://u:p@cfghost/db"}).database.display.endswith("@envhost/db")


@pytest.mark.parametrize("database, match", [
    ({"uri": "x"}, "unknown keys: uri"),
    ({"url": "not a url"}, "not a valid database URL"),
    ("sqlite", "must be a mapping"),
    ({"url": "sqlite:///x.db", "namespace": "n" * 300}, "longer than"),
])
def test_bad_database_config(work, no_env_url, database, match):
    with pytest.raises(ConfigError, match=match):
        _config(work, database)


def test_unsupported_backend_rejected(tmp_path, no_env_url):
    db = DatabaseConfig({"url": "oracle+oracledb://u:p@h/db"}, tmp_path / "runs")
    with pytest.raises(ConfigError, match="unsupported database 'oracle'"):
        get_engine(db.url)


def test_namespaces_share_one_database(tmp_path, no_env_url):
    url = f"sqlite:///{tmp_path / 'shared.db'}"
    a = RunIndex(tmp_path / "a" / "runs", DatabaseConfig({"url": url, "namespace": "a"}, tmp_path / "a" / "runs"))
    b = RunIndex(tmp_path / "b" / "runs", DatabaseConfig({"url": url, "namespace": "b"}, tmp_path / "b" / "runs"))
    a.save({"run_id": "same-id", "status": "active", "site": "A"})
    b.save({"run_id": "same-id", "status": "finished", "site": "B"})
    b.save({"run_id": "b2", "status": "active"})
    assert a.get("same-id")["site"] == "A" and b.get("same-id")["site"] == "B"
    assert [r["run_id"] for r in a.query()] == ["same-id"]
    assert a.rebuild() == 1                     # only a's runs directory, only a's rows
    assert b.counts() == {"finished": 1, "active": 1}
    a.remove("same-id")
    assert a.get("same-id") is None and b.get("same-id")["site"] == "B"


def test_sqlite_has_one_writer_at_a_time(tmp_path, no_env_url):
    """While one process holds the write lock, another process's save waits: neither its
    state.json nor its row appears until the lock is released."""
    runs = tmp_path / "runs"
    index = RunIndex(runs)
    index.save({"run_id": "first", "status": "active"})   # tables exist; the child only writes
    child = r'''
import sys
from globusiri.runindex import RunIndex
RunIndex(sys.argv[1]).save({"run_id": "second", "status": "active"})
'''
    with write_lock(runs):
        proc = subprocess.Popen([sys.executable, "-c", child, str(runs)], cwd=REPO,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(1.0)
        assert proc.poll() is None, proc.communicate()
        assert not (runs / "second" / "state.json").exists()
        assert index.get("second") is None          # reads take no lock
    out, err = proc.communicate(timeout=30)
    assert proc.returncode == 0, out + err
    assert index.get("second")["status"] == "active"


class _FakeSession:
    def __init__(self, dialect):
        self.dialect, self.statements = dialect, []

    def get_bind(self):
        return self

    def execute(self, stmt):
        self.statements.append(str(stmt.compile(dialect=self.dialect)))


def test_mysql_upsert_and_schema_sql():
    s = _FakeSession(mysql.dialect())
    _upsert(s, "ns", {"run_id": "r1", "status": "active", "steps": {"submit": {"job_id": 42}}})
    sql = s.statements[0]
    assert sql.startswith("INSERT INTO globusiri_runs") and "ON DUPLICATE KEY UPDATE" in sql
    assert "run_id = " not in sql.split("ON DUPLICATE KEY UPDATE")[1]  # the key is not updated
    ddl = str(CreateTable(Run.__table__).compile(dialect=mysql.dialect()))
    exact = "VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL"
    assert f"namespace {exact}" in ddl and f"run_id {exact}" in ddl
    assert "PRIMARY KEY (namespace, run_id)" in ddl and "message TEXT" in ddl
    assert "COLLATE" not in str(CreateTable(Run.__table__).compile(dialect=sqlite.dialect()))
