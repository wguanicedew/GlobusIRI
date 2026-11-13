import json
import time

import globus_sdk
import pytest
import yaml

from globusiri import panda_secrets
from globusiri.errors import LoginRequired
from globusiri.secure_io import is_private
from globusiri.tokens import manager
from globusiri.tokens.e2e import build_transfer_scope
from globusiri.tokens.iri import FACILITIES

from .conftest import write_access


def api_error(status, body):
    """A real globus_sdk.GlobusAPIError built from an HTTP response."""
    import requests

    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body).encode()
    r.headers["Content-Type"] = "application/json"
    r.request = requests.Request("GET", "https://x").prepare()
    r.url = "https://x"
    return globus_sdk.GlobusAPIError(r)


def test_consent_required_detection():
    from globusiri.clients.transfer import consent_required

    err = api_error(403, {"code": "ConsentRequired", "message": "m", "required_scopes": ["s1"]})
    assert consent_required(err) == ["s1"]
    assert consent_required(api_error(403, {"code": "PermissionDenied", "message": "m"})) is None


class FakeTokenResponse:
    def __init__(self, data, by_rs=None):
        self.data = data
        self.by_resource_server = by_rs or {}


@pytest.fixture
def refresh_calls(monkeypatch):
    """Patch NativeAppAuthClient.oauth2_refresh_token; tests push responses into the list."""
    queue, calls = [], []

    def fake(self, refresh_token, **kw):
        calls.append((self.client_id, refresh_token))
        r = queue.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(globus_sdk.NativeAppAuthClient, "oauth2_refresh_token", fake)
    return queue, calls


def test_https_refresh_writes_private_access_file(cfg, refresh_calls):
    queue, calls = refresh_calls
    m = manager("https", cfg, cfg.site("ALCF_CRUX"))
    m.write_refresh({"client_id": "cid", "refresh_token": "RT", "collection_id": "COLL", "https_server": "https://g"})
    exp = int(time.time()) + 3600 * 48
    queue.append(FakeTokenResponse({}, {"COLL": {"access_token": "AT", "expires_at_seconds": exp, "refresh_token": "RT2"}}))
    res = m.refresh()
    assert res.refreshed and res.expires_at == exp
    data = yaml.safe_load(m.access_file.read_text())
    assert data == {"client_id": "cid", "access_token": "AT", "expires_at": exp, "collection_id": "COLL",
                    "https_server": "https://g"}
    assert is_private(m.access_file)
    assert yaml.safe_load(m.refresh_file.read_text())["refresh_token"] == "RT2"  # rotated token kept
    assert calls == [("cid", "RT")]


def test_refresh_skipped_when_fresh(cfg, refresh_calls):
    queue, calls = refresh_calls
    m = manager("https", cfg, cfg.site("NERSC_Perlmutter"))
    write_access(m.access_file, expires_at=int(time.time()) + 40 * 3600)
    res = m.refresh(min_lifetime=6 * 3600)
    assert not res.refreshed and calls == []


def test_refresh_without_refresh_token_needs_login(cfg):
    m = manager("e2e", cfg)
    with pytest.raises(LoginRequired) as ei:
        m.refresh()
    assert ei.value.fix == "globusiri e2e login"


def test_iri_refresh_facility_token_in_other_tokens(cfg, refresh_calls):
    queue, calls = refresh_calls
    site = cfg.site("NERSC_Perlmutter")
    m = manager("iri", cfg, site)
    scope = FACILITIES["nersc"]["scope"]
    m.write_refresh({"access_token": "auth", "scope": "openid", "refresh_token": "AUTH_RT",
                     "other_tokens": [{"scope": scope, "access_token": "old", "refresh_token": "IRI_RT"}]})
    queue.append(FakeTokenResponse({"scope": scope, "access_token": "NEW", "expires_in": 3600}))
    res = m.refresh(validate=False)
    access = yaml.safe_load(m.access_file.read_text())
    assert access["access_token"] == "NEW"
    assert access["base_url"] == site.base_url and access["resource_id"] == site.resource_id
    assert abs(access["expires_at"] - (time.time() + 3600)) < 5
    stored = json.loads(m.refresh_file.read_text())
    assert stored["other_tokens"][0]["access_token"] == "NEW"
    assert stored["other_tokens"][0]["refresh_token"] == "IRI_RT"  # not rotated -> kept
    assert stored["refresh_token"] == "AUTH_RT"
    assert calls[0][1] == "IRI_RT" and res.refreshed


def test_iri_refresh_tries_other_client_ids(cfg, refresh_calls):
    queue, calls = refresh_calls
    m = manager("iri", cfg, cfg.site("ALCF_CRUX"))
    scope = FACILITIES["alcf"]["scope"]
    m.write_refresh({"scope": scope, "access_token": "a", "refresh_token": "RT"})

    queue += [api_error(401, {"error": "invalid_grant"}), FakeTokenResponse({"scope": scope, "access_token": "OK", "expires_in": 10})]
    m.refresh(validate=False)
    assert [c[0] for c in calls] == [FACILITIES["alcf"]["client_id"], FACILITIES["nersc"]["client_id"]]
    assert yaml.safe_load(m.access_file.read_text())["access_token"] == "OK"


def test_iri_facility_shared_by_sites(cfg):
    a = manager("iri", cfg, cfg.site("NERSC_Perlmutter"))
    assert a.access_file.name == "nersc.access.yaml"


def test_e2e_scope_has_optional_dependencies():
    s = str(build_transfer_scope(["E1"]))
    assert s.startswith("urn:globus:auth:scope:transfer.api.globus.org:all[")
    assert "*https://auth.globus.org/scopes/E1/data_access" in s
    assert "*https://auth.globus.org/scopes/E1/https" in s


def test_panda_decode_backward_compatible():
    assert panda_secrets.decode("bare-token") == {"access_token": "bare-token"}
    assert panda_secrets.decode('{"access_token": "x", "expires_at": 5}')["expires_at"] == 5


def test_panda_upload_download_roundtrip(cfg, monkeypatch):
    store = {}

    class FakePandaClient:
        @staticmethod
        def set_user_secret(key, value):
            store[key] = value
            return 0, (True, "ok")

        @staticmethod
        def get_user_secrets():
            return 0, (True, dict(store))

    monkeypatch.setattr(panda_secrets, "_client", lambda: FakePandaClient)
    m = manager("e2e", cfg)
    write_access(m.access_file, client_id="c", NERSC_https="https://n")
    m.upload_to_panda()
    payload = json.loads(store["USATLAS_HPC_TRANSFER_TOKEN"])
    assert payload["endpoints"] == {"NERSC": "https://n"}  # old-script layout too
    m.access_file.unlink()
    data = m.download_from_panda()
    assert data["NERSC_https"] == "https://n" and data["access_token"] == "TOKEN-SECRET-123"
    assert is_private(m.access_file)

    # old-format bare IRI secret
    store["NERSC_IRI_ACCESS"] = "raw-token"
    im = manager("iri", cfg, cfg.site("NERSC_Perlmutter"))
    data = im.download_from_panda()
    assert data["access_token"] == "raw-token" and data["base_url"] == "https://api.iri.nersc.gov"


def test_upload_only_when_changed(cfg, monkeypatch):
    uploads = []
    monkeypatch.setattr(panda_secrets, "set_secret", lambda k, p: uploads.append(k))
    m = manager("https", cfg, cfg.site("NERSC_Perlmutter"))
    write_access(m.access_file, expires_at=int(time.time()) + 40 * 3600)
    assert m.refresh(min_lifetime=3600, upload_to_panda=True).uploaded
    assert not m.refresh(min_lifetime=3600, upload_to_panda=True).uploaded
    assert uploads == ["NERSC_HTTPS_TOKEN"]


def test_require_sessions_asks_one_login_per_domain(monkeypatch, capsys):
    from globusiri.tokens.base import require_sessions

    prompts = []
    monkeypatch.setattr("builtins.input", lambda msg="": prompts.append(msg) or "")
    require_sessions("165ae151-86aa-430a-8251-ce565e51998f", ["bnl.gov", "nersc.gov", "bnl.gov"], what="e2e")
    out = capsys.readouterr().out
    assert len(prompts) == 2  # duplicates collapsed
    assert "session_required_single_domain=bnl.gov" in out and "session_required_single_domain=nersc.gov" in out
    assert out.index("bnl.gov") < out.index("nersc.gov")


def test_e2e_login_requires_endpoint_domains_before_token(cfg, monkeypatch):
    from globusiri.config import Endpoint
    from globusiri.tokens import e2e as e2e_tokens

    cfg.e2e.endpoints = {"BNL": Endpoint("BNL", {"id": "a" * 8, "data_access": True, "session_domains": "bnl.gov"}),
                         "NERSC": Endpoint("NERSC", {"id": "b" * 8, "session_domains": ["nersc.gov"]})}
    order = []

    class Resp:
        by_resource_server = {"transfer.api.globus.org": {"refresh_token": "RT"}}

    monkeypatch.setattr(e2e_tokens, "transfer_login", lambda *a: object())
    monkeypatch.setattr(e2e_tokens.E2ETokenManager, "resolve_endpoints", lambda self, tc: {
        label: {"id": ep.id, "https_server": None, "data_access": ep.data_access,
                "session_domains": ep.session_domains} for label, ep in cfg.e2e.endpoints.items()})
    monkeypatch.setattr(e2e_tokens, "require_sessions", lambda cid, domains, **kw: order.append(("sessions", domains)))
    monkeypatch.setattr(e2e_tokens, "interactive_login",
                        lambda *a, **kw: order.append(("token", kw["prompt_login"])) or Resp())
    monkeypatch.setattr(e2e_tokens.E2ETokenManager, "_do_refresh", lambda self: 0)
    manager("e2e", cfg).login(force=True, prompt_login=True)
    assert order == [("sessions", ["bnl.gov", "nersc.gov"]), ("token", False)]
