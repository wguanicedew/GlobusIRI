"""Relational storage of the run index (SQLAlchemy): SQLite by default, MySQL/MariaDB optional.

Configuration (sites.yaml, optional)::

    database:
      url: mysql+pymysql://harvester:${GLOBUSIRI_DB_PASSWORD}@dbhost:3306/harvester
      namespace: harvester-prod     # rows of this WORK_DIR (default: see below)
      pool_recycle: 3600            # MySQL only; also pool_size, max_overflow, pool_timeout

$GLOBUSIRI_DATABASE_URL overrides database.url. Without either, the index is the SQLite
file <WORK_DIR>/runs/index.db.
"""

import hashlib
import os
from pathlib import Path

from ..errors import ConfigError
from .models import KEY_LEN
from .session import backend, safe_url

ENV_URL = "GLOBUSIRI_DATABASE_URL"
SQLITE_NAME = "index.db"
OPTION_KEYS = ("url", "namespace", "pool_size", "max_overflow", "pool_timeout", "pool_recycle")


def default_sqlite_url(runs_dir):
    return f"sqlite:///{Path(runs_dir).resolve() / SQLITE_NAME}"


class DatabaseConfig:
    """Where the run index lives, and which rows belong to this WORK_DIR (namespace)."""

    def __init__(self, raw=None, runs_dir=None):
        raw = raw or {}
        if not isinstance(raw, dict):
            raise ConfigError("'database' must be a mapping")
        unknown = set(raw) - set(OPTION_KEYS)
        if unknown:
            raise ConfigError(f"database has unknown keys: {', '.join(sorted(unknown))} "
                              f"(known: {', '.join(OPTION_KEYS)})")
        self.runs_dir = Path(runs_dir) if runs_dir else None
        url = os.environ.get(ENV_URL) or raw.get("url")
        if not url:
            if self.runs_dir is None:
                raise ConfigError("no database.url and no runs directory for the default SQLite index")
            url = default_sqlite_url(self.runs_dir)
        self.url = str(url)
        try:
            self.backend = backend(self.url)
        except Exception as exc:  # noqa: BLE001 - sqlalchemy's ArgumentError; never echo a password
            raise ConfigError(f"database.url is not a valid database URL ({type(exc).__name__})") from None
        self.options = {k: raw[k] for k in ("pool_size", "max_overflow", "pool_timeout", "pool_recycle")
                        if raw.get(k) is not None}
        self.namespace = self._namespace(raw.get("namespace"))

    def _namespace(self, explicit):
        if explicit:
            ns = str(explicit)
            if len(ns) > KEY_LEN:
                raise ConfigError(f"database.namespace is longer than {KEY_LEN} characters")
            return ns
        if self.backend == "sqlite" or self.runs_dir is None:
            return "default"  # the file belongs to one WORK_DIR
        # A shared server: the WORK_DIR's runs directory tells WORK_DIRs apart. Hosts sharing
        # one WORK_DIR (shared filesystem) get the same namespace when they mount it at the
        # same path; otherwise set database.namespace on each.
        ns = str(self.runs_dir.resolve())
        if len(ns) > KEY_LEN:
            ns = "sha1:" + hashlib.sha1(ns.encode()).hexdigest()
        return ns

    @property
    def sqlite_path(self):
        """The SQLite file, or None for a server database."""
        if self.backend != "sqlite":
            return None
        from sqlalchemy.engine import make_url
        return Path(make_url(self.url).database)

    @property
    def display(self):
        return safe_url(self.url)

    def __repr__(self):
        return f"DatabaseConfig({self.display}, namespace={self.namespace!r})"
