#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Engines, transactional sessions and the write lock of the run index.

Backends:

* SQLite (default): ``sqlite:///<WORK_DIR>/runs/index.db``. SQLite allows one writer at a
  time; instead of relying on its busy timeout, every write takes an exclusive POSIX lock
  (``write_lock``) so only one process (or thread) writes at a time. Reads take no lock:
  the database runs in WAL mode, so readers see the last committed state while a writer
  works.
* MySQL / MariaDB (e.g. the Harvester database): ``mysql+pymysql://user:pw@host/db`` or
  ``mysql+mysqldb://...``. The server handles concurrent writers with transactions; the
  write lock is still taken to order a state.json write with its row (see RunIndex.save).

Engines are cached per URL and process (a forked child makes its own, never reusing the
parent's connections).
"""

import fcntl
import logging
import os
import threading
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from ..errors import ConfigError, DatabaseError

log = logging.getLogger("globusiri.orm")

LOCK_NAME = ".index.lock"
SQLITE_TIMEOUT = 30  # seconds; the write lock makes waiting on SQLite's own lock rare

_ENGINES = {}
_ENGINES_LOCK = threading.Lock()


def safe_url(url):
    """The URL without its password, for messages and `globusiri paths`."""
    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:  # noqa: BLE001 - never leak a malformed URL with a password
        return "<invalid database url>"


def backend(url):
    """'sqlite', 'mysql' (also MariaDB), or the SQLAlchemy dialect name."""
    name = make_url(url).get_backend_name()
    return "mysql" if name in ("mysql", "mariadb") else name


def _sqlite_module():
    try:
        import sqlite3
        return sqlite3
    except ImportError:  # Python built without _sqlite3 (missing SQLite headers at build time)
        try:
            from pysqlite3 import dbapi2
            return dbapi2
        except ImportError:
            raise ConfigError(
                "globusiri's default run index needs SQLite, but this Python was built without the _sqlite3 "
                "module. Install the SQLite dev package (libsqlite3-dev / sqlite-devel) and rebuild Python, "
                "run: pip install 'globusiri[sqlite]' (pysqlite3-binary), or configure database.url "
                "(MySQL/MariaDB)") from None


def _sqlite_on_connect(dbapi_con, _record):
    cur = dbapi_con.cursor()
    try:
        if cur.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
            cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")  # durable with WAL; the JSON files are authoritative
    finally:
        cur.close()


def _create_engine(url, options):
    u = make_url(url)
    kind = backend(url)
    params = {"hide_parameters": True}  # errors never carry values (state messages, ...)
    if kind == "sqlite":
        db = u.database
        if not db or db == ":memory:":
            raise ConfigError("database.url: a SQLite index must be a file (sqlite:///PATH)")
        Path(db).parent.mkdir(parents=True, exist_ok=True)
        params.update(module=_sqlite_module(), poolclass=NullPool,
                      connect_args={"timeout": SQLITE_TIMEOUT, "check_same_thread": False})
        engine = create_engine(u, **params)
        event.listen(engine, "connect", _sqlite_on_connect)
        return engine
    if kind == "mysql":
        params.update(pool_pre_ping=True, pool_recycle=int(options.get("pool_recycle") or 3600))
        for key in ("pool_size", "max_overflow", "pool_timeout"):
            if options.get(key) is not None:
                params[key] = int(options[key])
        try:
            return create_engine(u, **params)
        except ImportError as exc:
            raise ConfigError(
                f"database.url {safe_url(url)} needs a MySQL driver ({exc}); "
                "run: pip install 'globusiri[mysql]' (PyMySQL, for mysql+pymysql://), "
                "or install mysqlclient for mysql+mysqldb://") from None
    raise ConfigError(f"database.url: unsupported database '{u.get_backend_name()}' "
                      "(use sqlite:///PATH or mysql+pymysql://USER:PASSWORD@HOST[:PORT]/DB)")


def get_engine(url, options=None):
    key = (str(url), os.getpid())
    engine = _ENGINES.get(key)
    if engine is None:
        with _ENGINES_LOCK:
            engine = _ENGINES.get(key)
            if engine is None:
                engine = _create_engine(url, options or {})
                _ENGINES[key] = engine
    return engine


def dispose_engines():
    """Close every cached engine of this process (tests, or after the config changed)."""
    with _ENGINES_LOCK:
        for (_, pid), engine in list(_ENGINES.items()):
            if pid == os.getpid():
                engine.dispose()
        _ENGINES.clear()


@contextmanager
def write_lock(lock_dir):
    """Exclusive lock on <lock_dir>/.index.lock: one writer at a time, across processes and
    threads (each holder opens its own file description). Held only for local
    bookkeeping, never for remote API calls."""
    lock_dir = Path(lock_dir)
    lock_dir.mkdir(parents=True, exist_ok=True)
    with open(lock_dir / LOCK_NAME, "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


@contextmanager
def session_scope(engine):
    """A transaction: commit on success, rollback on any error. Database errors are raised
    as errors.DatabaseError (without statement values)."""
    try:
        with Session(engine, expire_on_commit=False) as session:
            with session.begin():
                yield session
    except SQLAlchemyError as exc:
        log.error("run index database %s: %s", safe_url(engine.url), exc)
        raise DatabaseError(f"run index database {safe_url(engine.url)}: {exc}") from exc
