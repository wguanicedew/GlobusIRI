"""Central bookkeeping of workflow runs: <WORK_DIR>/runs/index.db (SQLite).

Each run's full record stays in runs/<run_id>/state.json. The index holds one summary row
per run, written by RunState.save() every time that file is written, so:

* `monitor` / `daemon` find the active runs with one query instead of reading every
  state.json (finished runs cost nothing);
* `runs` lists runs from the index alone.

The index is derived data. If it is missing it is rebuilt by scanning runs/*/state.json
once, and `globusiri runs --reindex` rebuilds it on demand (e.g. after copying runs in).

An index lock serializes initialization, state publication, and rebuilds. SQLite
transactions protect index updates; state.json is replaced atomically. Keep WORK_DIR
on a local filesystem with working POSIX locks and SQLite WAL support.
"""

import fcntl
import json
import logging
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path

from .secure_io import write_json_private

log = logging.getLogger("globusiri.runindex")

INDEX_NAME = "index.db"

COLUMNS = ("run_id", "site", "status", "current_step", "step_status", "step_state", "job_id", "created_at",
           "updated_at", "finished_at", "message")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id       TEXT PRIMARY KEY,
    site         TEXT,
    status       TEXT,          -- active | finished | failed | cancelled | NULL (made by `job submit`)
    current_step TEXT,
    step_status  TEXT,
    step_state   TEXT,          -- IRI job state while monitoring
    job_id       TEXT,
    created_at   TEXT,
    updated_at   TEXT,
    finished_at  TEXT,
    message      TEXT           -- last_error while active, final message otherwise
);
CREATE INDEX IF NOT EXISTS runs_status ON runs (status, created_at);
CREATE INDEX IF NOT EXISTS runs_site ON runs (site, created_at);
"""


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


class RunIndex:
    def __init__(self, runs_dir):
        self.runs_dir = Path(runs_dir)
        self.path = self.runs_dir / INDEX_NAME

    @contextmanager
    def _db(self):
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        # Hold this only for local bookkeeping, never for remote API calls. In
        # particular, a rebuild cannot read old JSON and overwrite a newer row.
        with open(self.runs_dir / ".index.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with closing(sqlite3.connect(str(self.path), timeout=30)) as con:
                con.row_factory = sqlite3.Row
                self.path.chmod(0o600)
                if con.execute("PRAGMA journal_mode").fetchone()[0] != "wal":
                    con.execute("PRAGMA journal_mode=WAL")
                con.executescript(_SCHEMA)
                with con:
                    # Persist initialization inside the same transaction as the
                    # import, so interrupted initialization is retried next time.
                    if con.execute("PRAGMA user_version").fetchone()[0] == 0:
                        self._scan_into(con)
                        con.execute("PRAGMA user_version=1")
                    yield con

    def _scan_into(self, con):
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
            self._upsert(con, data)
            n += 1
        if n:
            log.info("index: added %d existing runs to %s", n, self.path)
        return n

    @staticmethod
    def _upsert(con, data):
        row = summary(data)
        cols = ", ".join(COLUMNS)
        marks = ", ".join(f":{c}" for c in COLUMNS)
        updates = ", ".join(f"{c}=excluded.{c}" for c in COLUMNS if c != "run_id")
        con.execute(f"INSERT INTO runs ({cols}) VALUES ({marks}) ON CONFLICT(run_id) DO UPDATE SET {updates}", row)

    # ------------------------------------------------------------------ public
    def save(self, data):
        """Publish complete state and its summary under the same bookkeeping lock.

        JSON is authoritative. A process killed between the file replacement and
        SQLite commit can leave an old index row; rebuild() repairs that gap.
        """
        with self._db() as con:
            write_json_private(self.runs_dir / data["run_id"] / "state.json", data)
            self._upsert(con, data)

    def upsert(self, data):
        with self._db() as con:
            self._upsert(con, data)

    def query(self, *, status=None, site=None, limit=None, oldest_first=False):
        """Summary rows (dicts), newest first unless oldest_first. status may be a list."""
        where, args = [], []
        if status:
            statuses = [status] if isinstance(status, str) else list(status)
            where.append(f"status IN ({', '.join('?' * len(statuses))})")
            args += statuses
        if site:
            where.append("site = ?")
            args.append(site)
        sql = "SELECT * FROM runs"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY created_at {'ASC' if oldest_first else 'DESC'}, run_id"
        if limit:
            sql += " LIMIT ?"
            args.append(int(limit))
        with self._db() as con:
            return [dict(r) for r in con.execute(sql, args)]

    def get(self, run_id):
        with self._db() as con:
            r = con.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            return dict(r) if r else None

    def counts(self):
        with self._db() as con:
            return {r["status"]: r["n"] for r in con.execute("SELECT status, COUNT(*) AS n FROM runs GROUP BY status")}

    def remove(self, run_id):
        with self._db() as con:
            con.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))

    def rebuild(self):
        """Re-read runs/*/state.json in one transaction. Returns the number of runs."""
        with self._db() as con:
            con.execute("DELETE FROM runs")
            return self._scan_into(con)
