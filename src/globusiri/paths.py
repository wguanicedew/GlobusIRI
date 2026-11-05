"""WORK_DIR resolution and the conventional locations of local files.

WORK_DIR is ``$GLOBUSIRI_WORK_DIR`` if set, else the per-user globusiri data dir
(``platformdirs.user_data_dir("globusiri")``). It is deliberately never the
installed package directory, which ``pip install --upgrade`` replaces.
"""

import os
from importlib import resources
from pathlib import Path

import platformdirs

from .errors import ConfigError

ENV_WORK_DIR = "GLOBUSIRI_WORK_DIR"
ENV_CONFIG = "GLOBUSIRI_CONFIG"
APP_NAME = "globusiri"


def work_dir_source():
    """Return (path, source) where source says how the path was chosen."""
    env = os.environ.get(ENV_WORK_DIR)
    if env:
        return Path(env).expanduser().resolve(), f"${ENV_WORK_DIR}"
    return Path(platformdirs.user_data_dir(APP_NAME)), "per-user data dir"


def work_dir():
    return work_dir_source()[0]


def package_data_dir():
    """Read-only data shipped in the wheel (example config, job files, templates)."""
    return Path(str(resources.files("globusiri") / "data"))


class Paths:
    """All conventional local paths under one WORK_DIR."""

    def __init__(self, root=None):
        self.root = Path(root).expanduser().resolve() if root else work_dir()

    # directories
    @property
    def config_dir(self):
        return self.root / "config"

    @property
    def tokens_dir(self):
        return self.root / "tokens"

    @property
    def logs_dir(self):
        return self.root / "logs"

    @property
    def runs_dir(self):
        return self.root / "runs"

    @property
    def templates_dir(self):
        return self.root / "templates"

    @property
    def jobs_dir(self):
        return self.root / "jobs"

    @property
    def inputs_dir(self):
        return self.root / "inputs"

    # files
    @property
    def default_config(self):
        return self.config_dir / "sites.yaml"

    def iri_refresh(self, facility):
        return self.tokens_dir / "iri" / f"{facility}.refresh.json"

    def iri_access(self, facility):
        return self.tokens_dir / "iri" / f"{facility}.access.yaml"

    def https_refresh(self, site):
        return self.tokens_dir / "https" / f"{site}.refresh.yaml"

    def https_access(self, site):
        return self.tokens_dir / "https" / f"{site}.access.yaml"

    @property
    def e2e_refresh(self):
        return self.tokens_dir / "e2e" / "refresh.yaml"

    @property
    def e2e_access(self):
        return self.tokens_dir / "e2e" / "access.yaml"

    @property
    def panda_sync(self):
        return self.tokens_dir / ".panda_sync.json"

    @property
    def log_file(self):
        return self.logs_dir / "globusiri.log"

    def run_dir(self, run_id):
        return self.runs_dir / run_id

    # helpers
    def resolve(self, path):
        """Expand ${VAR}/~ and resolve a relative local path against WORK_DIR."""
        if path is None:
            return None
        p = Path(os.path.expandvars(str(path))).expanduser()
        return p if p.is_absolute() else self.root / p

    def find_template(self, name):
        p = Path(os.path.expandvars(str(name))).expanduser()
        if p.is_absolute():
            candidates = [p]
        else:
            candidates = [self.templates_dir / p, package_data_dir() / "templates" / p, self.root / p]
        for c in candidates:
            if c.is_file():
                return c
        raise ConfigError(f"template '{name}' not found (looked in: {', '.join(str(c) for c in candidates)})")

    def find_job_file(self, name):
        p = Path(os.path.expandvars(str(name))).expanduser()
        candidates = [p] if p.is_absolute() else [p, self.jobs_dir / p]
        for c in candidates:
            if c.is_file():
                return c.resolve()
        raise ConfigError(f"job file '{name}' not found (looked in: {', '.join(str(c) for c in candidates)})")

    def config_file(self, explicit=None):
        """Return (path, source) of the site config."""
        if explicit:
            return Path(explicit).expanduser(), "--config"
        env = os.environ.get(ENV_CONFIG)
        if env:
            return Path(env).expanduser(), f"${ENV_CONFIG}"
        return self.default_config, "WORK_DIR"

    def ensure(self):
        """Create WORK_DIR and its subdirectories (tokens/ is 0700)."""
        for d in (self.root, self.config_dir, self.logs_dir, self.runs_dir, self.templates_dir, self.jobs_dir, self.inputs_dir):
            d.mkdir(parents=True, exist_ok=True)
        for d in (self.tokens_dir, self.tokens_dir / "iri", self.tokens_dir / "https", self.tokens_dir / "e2e"):
            d.mkdir(parents=True, exist_ok=True)
            os.chmod(d, 0o700)
