import shutil
import time

import pytest
import yaml

from globusiri.config import Config
from globusiri.paths import Paths, package_data_dir


@pytest.fixture
def work(tmp_path, monkeypatch):
    """A fresh WORK_DIR with the example config and job files."""
    root = tmp_path / "work"
    monkeypatch.setenv("GLOBUSIRI_WORK_DIR", str(root))
    monkeypatch.delenv("GLOBUSIRI_CONFIG", raising=False)
    paths = Paths(root)
    paths.ensure()
    shutil.copyfile(package_data_dir() / "sites.example.yaml", paths.default_config)
    for f in [*(package_data_dir() / "jobs").glob("*.yaml"), *(package_data_dir() / "jobs").glob("*.sh")]:
        shutil.copyfile(f, paths.jobs_dir / f.name)
    return paths


@pytest.fixture
def cfg(work):
    raw = yaml.safe_load(work.default_config.read_text())
    return Config(raw, path=work.default_config, paths=work)


def write_access(path, **data):
    path.parent.mkdir(parents=True, exist_ok=True)
    data.setdefault("access_token", "TOKEN-SECRET-123")
    data.setdefault("expires_at", int(time.time()) + 48 * 3600)
    path.write_text(yaml.safe_dump(data))
    path.chmod(0o600)


class FakeResponse:
    def __init__(self, status=200, json_data=None, text=None, headers=None, content=b""):
        self.status_code = status
        self._json = json_data
        self.headers = headers or ({"Content-Type": "application/json"} if json_data is not None else {})
        self.text = text if text is not None else ("" if json_data is None else str(json_data))
        self.content = content or (b"x" if json_data is not None else b"")
        self.reason = "reason"

    @property
    def ok(self):
        return self.status_code < 400

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json

    def iter_content(self, chunk_size=1):
        yield self.content

    def close(self):
        pass


class FakeSession:
    """Records requests; returns queued responses (or a default)."""

    def __init__(self, responses=None, default=None):
        self.responses = list(responses or [])
        self.default = default or FakeResponse(200, {})
        self.calls = []
        self.headers = {}

    def request(self, method, url, **kw):
        body = kw.get("data")
        if hasattr(body, "read"):
            kw["body_bytes"] = body.read()
        self.calls.append((method, url, kw))
        if self.responses:
            r = self.responses.pop(0)
            if isinstance(r, Exception):
                raise r
            return r
        return self.default
