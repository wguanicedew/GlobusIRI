"""Central bookkeeping of workflow runs: a relational index of runs/<run_id>/state.json.

The database is SQLite (<WORK_DIR>/runs/index.db) by default, or MySQL/MariaDB when
sites.yaml sets ``database.url`` (see orm/__init__.py). Each run's full record stays in
runs/<run_id>/state.json. The index holds one summary row per run, written by
RunState.save() every time that file is written, so:

* `monitor` / `daemon` find the active runs with one query instead of reading every
  state.json (finished runs cost nothing);
* `runs` lists runs from the index alone.

The index is derived data. The first use of a namespace imports runs/*/state.json once,
and `globusiri runs --reindex` rebuilds it on demand (e.g. after copying runs in).

Writes (state publication, initialization, rebuilds) hold an exclusive lock on
runs/.index.lock: with SQLite it makes this process the only writer; with every backend it
keeps a rebuild from reading old JSON and overwriting a newer row. Reads take no lock
(SQLite runs in WAL mode). Keep WORK_DIR on a local filesystem with working POSIX locks.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, func, select
from sqlalchemy.exc import SQLAlchemyError

from .errors import DatabaseError
from .orm import DatabaseConfig
from .orm.models import COLUMNS, Meta, Run, register_models
from .orm.session import get_engine, session_scope, write_lock
from .secure_io import write_json_private

log = logging.getLogger("globusiri.runindex")

INDEX_NAME = "index.db"
INITIALIZED = "initialized"

# (url, namespace, pid) whose tables exist and whose namespace was imported
_READY = set()


def summary(data):
    """Index row for a state.json dict."""
    steps = data.get("steps") or {}
    cur = data.get("current_step")
    st = steps.get(cur) or {}
    if data.get("status") == "active":
        message = (data.get("last_error") or {}).get("error")
    else:
        message = data.get("message")
    return {
        "run_id": data.get("run_id"),
        "site": data.get("site"),
        "status": data.get("status"),
        "current_step": cur,
        "step_status": st.get("status"),
        "step_state": st.get("state"),
        "job_id": (steps.get("submit") or {}).get("job_id"),
        "created_at": data.get("created_at"),
        "updated_at": data.get("updated_at"),
        "finished_at": data.get("finished_at"),
        "message": message,
    }


def _text(value):
    return None if value is None else str(value)


def _upsert(session, namespace, data):
    row = {k: _text(v) for k, v in summary(data).items()}
    row["namespace"] = namespace
    updates = [c for c in COLUMNS if c != "run_id"]
    table = Run.__table__
    dialect = session.get_bind().dialect.name
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert
        stmt = insert(table).values(**row)
        stmt = stmt.on_conflict_do_update(index_elements=["namespace", "run_id"],
                                          set_={c: stmt.excluded[c] for c in updates})
    elif dialect in ("mysql", "mariadb"):
        from sqlalchemy.dialects.mysql import insert
        stmt = insert(table).values(**row)
        stmt = stmt.on_duplicate_key_update(**{c: stmt.inserted[c] for c in updates})
    elif dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
        stmt = insert(table).values(**row)
        stmt = stmt.on_conflict_do_update(index_elements=["namespace", "run_id"],
                                          set_={c: stmt.excluded[c] for c in updates})
    else:
        session.merge(Run(**row))
        return
    session.execute(stmt)


class RunIndex:
    def __init__(self, runs_dir, database=None):
        self.runs_dir = Path(runs_dir)
        self.db = database if database is not None else DatabaseConfig(None, self.runs_dir)
        self.namespace = self.db.namespace
        self.path = self.db.sqlite_path  # the SQLite file; None for a server database

    @property
    def engine(self):
        return get_engine(self.db.url, self.db.options)

    # ------------------------------------------------------------------ setup
    def _ready(self):
        """Create the tables and import the existing runs of this namespace, once.

        Never call it while holding write_lock (the lock is not re-entrant)."""
        key = (self.db.url, self.namespace, os.getpid())
        if key in _READY and (self.path is None or self.path.exists()):
            return
        with write_lock(self.runs_dir):
            try:
                try:
                    register_models(self.engine)
                except SQLAlchemyError:
                    register_models(self.engine)  # another host created the tables meanwhile
            except SQLAlchemyError as exc:
                raise DatabaseError(f"run index database {self.db.display}: {exc}") from exc
            if self.path is not None:
                self.path.chmod(0o600)
            with session_scope(self.engine) as s:
                # The import and its marker commit together: an interrupted import is retried.
                if s.get(Meta, (self.namespace, INITIALIZED)) is None:
                    self._scan_into(s)
                    s.merge(Meta(namespace=self.namespace, key=INITIALIZED,
                                 value=datetime.now(timezone.utc).isoformat(timespec="seconds")))
        _READY.add(key)

    def _scan_into(self, session):
        n = 0
        for state_file in self.runs_dir.glob("*/state.json"):
            try:
                data = json.loads(state_file.read_text())
                if not isinstance(data, dict):
                    raise ValueError("state must be a JSON object")
            except (OSError, ValueError) as exc:
                log.warning("index: skip %s: %s", state_file, exc)
                continue
            data.setdefault("run_id", state_file.parent.name)
            _upsert(session, self.namespace, data)
            n += 1
        if n:
            log.info("index: added %d existing runs to %s", n, self.db.display)
        return n

    # ------------------------------------------------------------------ writes
    def save(self, data):
        """Publish complete state and its summary under the write lock.

        JSON is authoritative. A process killed between the file replacement and the
        database commit can leave an old index row; rebuild() repairs that gap.
        """
        self._ready()
        with write_lock(self.runs_dir), session_scope(self.engine) as s:
            write_json_private(self.runs_dir / data["run_id"] / "state.json", data)
            _upsert(s, self.namespace, data)

    def upsert(self, data):
        self._ready()
        with write_lock(self.runs_dir), session_scope(self.engine) as s:
            _upsert(s, self.namespace, data)

    def remove(self, run_id):
        self._ready()
        with write_lock(self.runs_dir), session_scope(self.engine) as s:
            s.execute(delete(Run).where(Run.namespace == self.namespace, Run.run_id == run_id))

    def rebuild(self):
        """Re-read runs/*/state.json in one transaction. Returns the number of runs."""
        self._ready()
        with write_lock(self.runs_dir), session_scope(self.engine) as s:
            s.execute(delete(Run).where(Run.namespace == self.namespace))
            return self._scan_into(s)

    # ------------------------------------------------------------------ reads
    def query(self, *, status=None, site=None, limit=None, oldest_first=False):
        """Summary rows (dicts), newest first unless oldest_first. status may be a list."""
        self._ready()
        stmt = select(Run).where(Run.namespace == self.namespace)
        if status:
            statuses = [status] if isinstance(status, str) else list(status)
            stmt = stmt.where(Run.status.in_(statuses))
        if site:
            stmt = stmt.where(Run.site == site)
        order = Run.created_at.asc() if oldest_first else Run.created_at.desc()
        stmt = stmt.order_by(order, Run.run_id)
        if limit:
            stmt = stmt.limit(int(limit))
        with session_scope(self.engine) as s:
            return [self._row(r) for r in s.scalars(stmt)]

    def get(self, run_id):
        self._ready()
        with session_scope(self.engine) as s:
            r = s.get(Run, (self.namespace, run_id))
            return self._row(r) if r else None

    def counts(self):
        self._ready()
        stmt = (select(Run.status, func.count()).where(Run.namespace == self.namespace)
                .group_by(Run.status))
        with session_scope(self.engine) as s:
            return {status: n for status, n in s.execute(stmt)}

    @staticmethod
    def _row(run):
        d = run.to_dict()
        return {c: d.get(c) for c in COLUMNS}
