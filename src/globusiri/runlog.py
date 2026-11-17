#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Per-run log files.

Every record a `globusiri.*` logger emits while code runs inside `run_logging(...)` also
goes to that run's log file. This covers IRI calls, HTTPS and E2E staging, step changes,
and job state changes. The global log (<WORK_DIR>/logs/globusiri.log) and any handlers
installed by an embedding application (e.g. Harvester) are unchanged.

The file is `job.log` from the job file if it is set. Relative paths resolve against
WORK_DIR, and `{run_id}` is replaced. Otherwise it is <WORK_DIR>/runs/<run_id>/globusiri.log.

The run is tracked in a context variable, so concurrent threads (one per run in Harvester)
each write only their own run's records.
"""

import contextvars
import logging
import os
import threading
from contextlib import contextmanager
from pathlib import Path

LOG_NAME = "globusiri.log"
FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

_current_run = contextvars.ContextVar("globusiri_run_id", default=None)
_logger = logging.getLogger("globusiri")
_level_lock = threading.Lock()
_level_users = 0
_saved_level = None


def run_log_path(config, run_id, job=None):
    spec = job.log if job is not None else None
    if spec:
        return config.paths.resolve(str(spec).replace("{run_id}", run_id))
    return config.paths.run_dir(run_id) / LOG_NAME


class _RunFilter(logging.Filter):
    def __init__(self, run_id):
        super().__init__()
        self.run_id = run_id

    def filter(self, record):
        return _current_run.get() == self.run_id


def _raise_verbosity():
    """Make sure INFO records are created while a run log is open. If the embedding
    application left `globusiri` at WARNING, lower it to INFO until the last run log closes."""
    global _level_users, _saved_level
    with _level_lock:
        if _level_users == 0 and not _logger.isEnabledFor(logging.INFO):
            _saved_level = _logger.level
            _logger.setLevel(logging.INFO)
        _level_users += 1


def _restore_verbosity():
    global _level_users, _saved_level
    with _level_lock:
        _level_users -= 1
        if _level_users == 0 and _saved_level is not None:
            _logger.setLevel(_saved_level)
            _saved_level = None


@contextmanager
def run_logging(path, run_id, level=logging.INFO):
    """Copy globusiri log records emitted in this context (thread) to `path`.
    Nesting with the same run is a no-op. If the file cannot be opened, the run goes on
    without it."""
    if _current_run.get() == run_id:
        yield
        return
    path = Path(path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    except OSError as exc:
        _logger.warning("run %s: cannot open run log %s: %s", run_id, path, exc)
        handler = None
    if handler is not None:
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(FORMAT))
        handler.addFilter(_RunFilter(run_id))
        _logger.addHandler(handler)
        _raise_verbosity()
    token = _current_run.set(run_id)
    try:
        yield
    finally:
        _current_run.reset(token)
        if handler is not None:
            _logger.removeHandler(handler)
            handler.close()
            _restore_verbosity()
