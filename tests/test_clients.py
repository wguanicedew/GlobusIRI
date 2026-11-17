import logging

import pytest
import requests

from globusiri.clients import http as http_mod
from globusiri.clients.http import TokenSource
from globusiri.clients.https import GlobusHttpsClient
from globusiri.clients.iri import IriClient
from globusiri.config import PathMap
from globusiri.errors import LoginRequired, RemoteError

from .conftest import FakeResponse, FakeSession, write_access


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(http_mod.time, "sleep", lambda s: None)


def make_iri(tmp_path, responses, debug=False):
    write_access(tmp_path / "iri.yaml")
    client = IriClient("https://api.example", TokenSource(tmp_path / "iri.yaml", label="IRI", fix="fix"),
                       resource_id="rid", debug=debug)
    client.session = FakeSession(responses)
    return client


def test_iri_retries_5xx_then_succeeds(tmp_path):
    c = make_iri(tmp_path, [FakeResponse(503), requests.ConnectionError("x"), FakeResponse(200, {"id": "J1"})])
    assert c.launch_job({"a": 1})["id"] == "J1"
    assert len(c.session.calls) == 3
    method, url, kw = c.session.calls[-1]
    assert method == "POST" and url == "https://api.example/api/v1/compute/job/rid"
    assert kw["headers"]["Authorization"] == "Bearer TOKEN-SECRET-123"


def test_iri_no_retry_on_4xx(tmp_path):
    c = make_iri(tmp_path, [FakeResponse(400, {"detail": "bad"})])
    with pytest.raises(RemoteError):
        c.get_job("J1")
    assert len(c.session.calls) == 1


def test_iri_401_is_login_required(tmp_path):
    c = make_iri(tmp_path, [FakeResponse(401, {"detail": "no"})])
    with pytest.raises(LoginRequired):
        c.stat("/x")


def test_iri_waits_for_async_task(tmp_path, monkeypatch):
    from globusiri.clients import iri as iri_mod

    monkeypatch.setattr(iri_mod.time, "sleep", lambda s: None)
    c = make_iri(tmp_path, [FakeResponse(200, {"task_id": "t", "task_uri": "https://api.example/task/t"}),
                            FakeResponse(200, {"status": "running"}),
                            FakeResponse(200, {"status": "completed", "result": {"ok": 1}})])
    assert c.mkdir("/w/r", parents=True)["status"] == "completed"
    assert c.session.calls[0][2]["json"] == {"path": "/w/r", "parent": True}


def test_iri_debug_redacts_token(tmp_path, caplog):
    c = make_iri(tmp_path, [FakeResponse(200, {})], debug=True)
    with caplog.at_level(logging.DEBUG, logger="globusiri"):
        c.get_job("J1")
    assert "TOKEN-SECRET-123" not in caplog.text
    assert "<redacted>" in caplog.text


def test_expired_token_is_login_required(tmp_path):
    write_access(tmp_path / "t.yaml", expires_at=1)
    with pytest.raises(LoginRequired, match="expired"):
        TokenSource(tmp_path / "t.yaml", label="X", fix="globusiri x refresh").token()


def test_token_source_reloads_on_change(tmp_path):
    import os

    p = tmp_path / "t.yaml"
    write_access(p, access_token="A")
    src = TokenSource(p, label="X", fix="f")
    assert src.token() == "A"
    write_access(p, access_token="B")
    os.utime(p, (os.path.getatime(p), os.path.getmtime(p) + 10))
    assert src.token() == "B"


def test_https_put_get_with_path_map(tmp_path):
    write_access(tmp_path / "h.yaml", https_server="https://g.example/")
    c = GlobusHttpsClient(TokenSource(tmp_path / "h.yaml", label="H", fix="f"), PathMap({"/lus/eagle/projects": "/"}))
    c.session = FakeSession([FakeResponse(200), FakeResponse(200, content=b"hello")])
    f = tmp_path / "up.txt"
    f.write_text("data")
    c.put(f, "/lus/eagle/projects/P/jobs/r1/executable_batch")
    method, url, kw = c.session.calls[0]
    assert (method, url) == ("PUT", "https://g.example/P/jobs/r1/executable_batch")
    assert kw["body_bytes"] == b"data"
    c.get("/lus/eagle/projects/P/jobs/r1/o.txt", tmp_path / "out" / "o.txt")
    assert (tmp_path / "out" / "o.txt").read_bytes() == b"hello"
    assert c.session.calls[1][1] == "https://g.example/P/jobs/r1/o.txt"


def test_https_403_is_login_required(tmp_path):
    write_access(tmp_path / "h.yaml", https_server="https://g.example")
    c = GlobusHttpsClient(TokenSource(tmp_path / "h.yaml", label="H", fix="f"), PathMap())
    c.session = FakeSession([FakeResponse(403, text="denied")])
    with pytest.raises(LoginRequired):
        c.get("/x", tmp_path / "x")


def test_iri_rm_waits_for_task(tmp_path, monkeypatch):
    from globusiri.clients import iri as iri_mod

    monkeypatch.setattr(iri_mod.time, "sleep", lambda s: None)
    c = make_iri(tmp_path, [FakeResponse(200, {"task_id": "T1", "task_uri": "https://api.example/task/T1"}),
                            FakeResponse(200, {"status": "completed", "result": None})])
    assert c.rm("/a/b")["status"] == "completed"
    method, url = c.session.calls[0][:2]
    assert method == "DELETE" and url == "https://api.example/api/v1/filesystem/rm/rid"
    assert c.session.calls[0][2]["params"] == {"path": "/a/b"}


SFAPI = "Exception: Error: 404: Unable to get sfapi data: 403"


def _task(tid):
    return FakeResponse(200, {"task_id": tid, "task_uri": f"https://api.example/task/{tid}"})


def test_iri_sfapi_task_failure_is_retried(tmp_path, monkeypatch):
    from globusiri.clients import iri as iri_mod

    sleeps = []
    monkeypatch.setattr(iri_mod.time, "sleep", sleeps.append)
    c = make_iri(tmp_path, [_task("t1"), FakeResponse(200, {"status": "failed", "result": SFAPI}),
                            _task("t2"), FakeResponse(200, {"status": "failed", "result": SFAPI}),
                            _task("t3"), FakeResponse(200, {"status": "completed", "result": {"output": {}}})])
    assert c.stat("/x")["status"] == "completed"
    assert [m for m, _, _ in c.session.calls].count("GET") == 6   # the stat was sent 3 times
    assert sleeps == [c.sfapi_retry_sleep] * 2


def test_iri_sfapi_gives_up_after_retries(tmp_path, monkeypatch):
    from globusiri.clients import iri as iri_mod

    monkeypatch.setattr(iri_mod.time, "sleep", lambda s: None)
    c = make_iri(tmp_path, [FakeResponse(403, {"detail": SFAPI})] * 3)
    with pytest.raises(iri_mod.SfapiUnavailable, match="failed 3 times in a row") as err:
        c.get_job("J1")
    assert not isinstance(err.value, LoginRequired)   # an HTTP 403 here is not a token problem
    assert isinstance(err.value, RemoteError) and err.value.status == 403
    assert len(c.session.calls) == 3


def test_iri_other_task_failure_not_retried(tmp_path, monkeypatch):
    from globusiri.clients import iri as iri_mod

    monkeypatch.setattr(iri_mod.time, "sleep", lambda s: None)
    c = make_iri(tmp_path, [_task("t1"), FakeResponse(200, {"status": "failed", "result": "No such file"})])
    with pytest.raises(RemoteError, match="No such file") as err:
        c.ls("/missing")
    assert not isinstance(err.value, iri_mod.SfapiUnavailable)
    assert len(c.session.calls) == 2


def test_iri_upload_retry_reopens_file(tmp_path, monkeypatch):
    from globusiri.clients import iri as iri_mod

    monkeypatch.setattr(iri_mod.time, "sleep", lambda s: None)
    src = tmp_path / "job.sh"
    src.write_text("#!/bin/bash\n")
    c = make_iri(tmp_path, [FakeResponse(404, {"detail": SFAPI}), FakeResponse(200, {"ok": True})])
    assert c.upload(src, "/w/job.sh") == {"ok": True}
    assert len(c.session.calls) == 2
