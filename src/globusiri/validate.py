#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Token validation: each token kind is checked against its own service.

Used by `globusiri validate`, by `refresh` (iri.validate) and by `run` before anything is
transferred or submitted. With a job, only the kinds the job requires are checked.
"""

import os
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import quote, urlencode

from . import panda_secrets, services
from .clients.transfer import consent_required
from .errors import GlobusIriError, LoginRequired, PandaError, RemoteError
from .tokens import E2ETokenManager, HttpsTokenManager, IriTokenManager

OK, WARN, FAIL, SKIP = "OK", "WARN", "FAIL", "SKIPPED"
ALCF_HOME_RESOURCE_ID = "6115bd2c-957a-4543-abff-5fae52992ff2"
VALIDATE_FILE = ".globusiri_validate"


@dataclass
class Check:
    site: str
    kind: str
    check: str
    status: str
    detail: str = ""
    fix: str = None
    expires_at: float = None

    def to_dict(self):
        return asdict(self)

    @property
    def detail_with_fix(self):
        return f"{self.detail}; run: {self.fix}" if self.fix and self.status in (WARN, FAIL) else self.detail


def _hours(seconds):
    return f"{seconds / 3600:.0f}h" if seconds >= 3600 else f"{max(seconds, 0) / 60:.0f}m"


# ---------------------------------------------------------------------- offline
def file_checks(mgr, site_name, min_lifetime=0):
    checks = []
    st = mgr.status()
    if not st.access_exists:
        fix = mgr.fix("refresh") if st.refresh_exists else mgr.fix("login")
        return [Check(site_name, mgr.kind, "token file", FAIL, f"{st.access_file} not found", fix)]
    if not st.access_private:
        checks.append(Check(site_name, mgr.kind, "file mode", WARN, f"{st.access_file} is readable by group/other",
                            f"chmod 600 {st.access_file}"))
    note = "" if st.refresh_exists else " (no refresh token here: runtime host)"
    remaining = st.remaining
    if st.expires_at is None:
        checks.append(Check(site_name, mgr.kind, "token file", WARN, f"expiry unknown{note}", mgr.fix("refresh")))
    elif remaining <= 0:
        checks.append(Check(site_name, mgr.kind, "token file", FAIL, f"expired {_hours(-remaining)} ago{note}",
                            mgr.fix("refresh"), st.expires_at))
    elif min_lifetime and remaining < min_lifetime:
        checks.append(Check(site_name, mgr.kind, "token file", WARN, f"expires in {_hours(remaining)}{note}",
                            mgr.fix("refresh"), st.expires_at))
    else:
        checks.append(Check(site_name, mgr.kind, "token file", OK, f"expires in {_hours(remaining)}{note}",
                            expires_at=st.expires_at))

    data = mgr.read_access()
    if not data.get("access_token"):
        checks.append(Check(site_name, mgr.kind, "content", FAIL, "no access_token", mgr.fix("refresh")))
    if mgr.kind == "iri":
        site = mgr.site
        if (data.get("base_url") or "").rstrip("/") != site.base_url:
            checks.append(Check(site_name, "iri", "content", FAIL,
                                f"base_url {data.get('base_url')} != {site.base_url} in sites.yaml", mgr.fix("refresh")))
        if data.get("resource_id") and data.get("resource_id") != site.resource_id:
            checks.append(Check(site_name, "iri", "content", WARN,
                                f"resource_id in token file differs from sites.yaml ({site.resource_id}); "
                                "the client always uses sites.yaml"))
    elif mgr.kind == "https":
        missing = [k for k in ("client_id", "collection_id", "https_server") if not data.get(k)]
        if missing:
            checks.append(Check(site_name, "https", "content", FAIL, f"missing {', '.join(missing)}", mgr.fix("login")))
        pinned = mgr.site.https_collection_id
        if pinned and data.get("collection_id") and pinned != data["collection_id"]:
            checks.append(Check(site_name, "https", "content", FAIL,
                                f"collection_id {data['collection_id']} != pinned {pinned}", mgr.fix("login") + " --force"))
    elif mgr.kind == "e2e" and not data.get("client_id"):
        checks.append(Check(site_name, "e2e", "content", FAIL, "missing client_id", mgr.fix("login")))
    return checks


# ---------------------------------------------------------------------- live: iri
def iri_validate_request(site):
    """(method, url, json_body) of the validation call: iri.validate_url (GET) if set; NERSC
    account/projects; ALCF filesystem ls on Home (v1: GET ?path=, v2: POST {"path": ...})."""
    iri = site.iri
    if iri.get("validate_url"):
        return "GET", iri["validate_url"], None
    api = f"{site.base_url}/api/{site.iri_api_version}"
    if site.facility == "nersc":
        return "GET", f"{api}/account/projects", None
    rid = iri.get("validate_resource_id") or ALCF_HOME_RESOURCE_ID
    path = iri.get("validate_path") or f"/home/{os.environ.get('USER') or os.environ.get('LOGNAME') or ''}/"
    url = f"{api}/filesystem/ls/{quote(rid, safe='')}"
    if site.iri_api_version == "v2":
        return "POST", url, {"path": path}
    return "GET", f"{url}?{urlencode({'path': path}, quote_via=quote, safe='/')}", None


def iri_api_check(config, site):
    fix = f"globusiri iri login {site.name} --force"
    method, url, body = iri_validate_request(site)
    short = url.replace(site.base_url, "")
    try:
        client = services.iri_client(config, site)
        resp = client.get_url(url, method=method, json_body=body)
    except LoginRequired as exc:
        return Check(site.name, "iri", "api", FAIL, str(exc.args[0]), exc.fix or fix)
    except GlobusIriError as exc:
        return Check(site.name, "iri", "api", FAIL, str(exc))
    if resp.status_code in (401, 403):
        return Check(site.name, "iri", "api", FAIL, f"{method} {short} -> {resp.status_code} (token rejected)", fix)
    if not resp.ok:
        return Check(site.name, "iri", "api", FAIL, f"{method} {short} -> {resp.status_code}: {resp.text[:200]}")
    try:
        body = resp.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        auths = (body.get("session_info") or {}).get("authentications") if isinstance(body.get("session_info"), dict) else None
        if isinstance(auths, dict) and not auths:
            return Check(site.name, "iri", "api", WARN,
                         f"{method} {short} -> {resp.status_code} but session_info.authentications is empty "
                         "(log in again in a private browser window)", fix)
    return Check(site.name, "iri", "api", OK, f"{method} {short} -> {resp.status_code}")


def iri_workdir_check(config, site):
    """stat remote.work_dir; if stat fails, check it with ls instead.

    ALCF does not implement stat (HTTP 501): ls then gives OK. NERSC's stat sometimes fails
    ("Unable to get sfapi data: 403") while ls works: that is a WARN, which does not block a
    submission (job submission itself uses mkdir/chmod, not stat).
    """
    wd = site.work_dir
    fix = f"globusiri iri login {site.name} --force"
    try:
        client = services.iri_client(config, site)
        try:
            client.stat(wd, resource_id=site.work_dir_resource_id)
            return Check(site.name, "iri", "work_dir", OK, f"stat {wd}")
        except RemoteError as stat_exc:
            try:
                client.ls(wd, resource_id=site.work_dir_resource_id)
            except RemoteError as ls_exc:
                if stat_exc.status == 501 and ls_exc.status == 501:
                    return Check(site.name, "iri", "work_dir", WARN,
                                 f"this IRI backend implements neither stat nor ls; {wd} not checked")
                return Check(site.name, "iri", "work_dir", FAIL, f"stat {wd}: {stat_exc}; ls: {ls_exc}")
            if stat_exc.status == 501:
                return Check(site.name, "iri", "work_dir", OK, f"ls {wd} (stat not implemented)")
            return Check(site.name, "iri", "work_dir", WARN, f"ls {wd} works, but stat failed: {stat_exc}")
    except LoginRequired as exc:
        return Check(site.name, "iri", "work_dir", FAIL, f"{wd}: {exc.args[0]}", fix)
    except GlobusIriError as exc:
        return Check(site.name, "iri", "work_dir", FAIL, f"{wd}: {exc}")


# ---------------------------------------------------------------------- live: https
def https_checks(config, site, write_test=False):
    fix = f"globusiri https login {site.name} --force"
    checks = []
    try:
        client = services.https_client(config, site)
        code, url = client.probe(site.work_dir)
    except LoginRequired as exc:
        return [Check(site.name, "https", "read work_dir", FAIL, str(exc.args[0]), exc.fix or fix)]
    except GlobusIriError as exc:
        return [Check(site.name, "https", "read work_dir", FAIL, str(exc))]
    globus_path = site.https_path_map.to_globus(site.work_dir)
    if code in (200, 204, 207):
        checks.append(Check(site.name, "https", "read work_dir", OK, f"GET {globus_path} -> {code}"))
    elif code in (401, 403):
        checks.append(Check(site.name, "https", "read work_dir", FAIL, f"GET {globus_path} -> {code} (token or consent)", fix))
    elif code == 404:
        checks.append(Check(site.name, "https", "read work_dir", FAIL,
                            f"GET {globus_path} -> 404 (check remote.work_dir and https.path_map)"))
    else:
        checks.append(Check(site.name, "https", "read work_dir", WARN, f"GET {globus_path} -> {code} (unexpected)"))

    if write_test:
        import posixpath

        remote = posixpath.join(site.work_dir, VALIDATE_FILE)
        payload = f"globusiri validate {time.time()}\n"
        with tempfile.TemporaryDirectory() as tmp:
            src, back = Path(tmp) / "up", Path(tmp) / "down"
            src.write_text(payload)
            try:
                client.put(src, remote)
                client.get(remote, back)
                ok = back.read_text() == payload
                checks.append(Check(site.name, "https", "write test", OK if ok else FAIL,
                                    f"PUT+GET {site.https_path_map.to_globus(remote)}" + ("" if ok else " (content differs)")))
            except LoginRequired as exc:
                checks.append(Check(site.name, "https", "write test", FAIL, str(exc.args[0]), fix))
            except GlobusIriError as exc:
                checks.append(Check(site.name, "https", "write test", FAIL, str(exc)))
    return checks


# ---------------------------------------------------------------------- live: e2e
def e2e_checks(config, sites):
    """get_endpoint on the endpoints these sites use, then ls their paths."""
    import globus_sdk

    fix = "globusiri e2e login --force"
    transfer = services.transfer(config)
    checks = []

    targets = {}  # (endpoint label, globus path) -> site name
    labels = set()
    for site in sites:
        if not site.has_e2e:
            continue
        ep = site.e2e_endpoint()
        labels.add(ep.label)
        if site.e2e_base_path:
            targets[(ep.label, ep.path_map.to_globus(site.e2e_base_path))] = site.name
        for sect, key, pkey in ((site.stagein, "src_endpoint", "src_path"), (site.stageout, "dst_endpoint", "dst_path")):
            if sect.get(key):
                labels.add(sect[key])
                if sect.get(pkey):
                    sep = config.e2e.endpoint(sect[key])
                    targets[(sep.label, sep.path_map.to_globus(sect[pkey]))] = site.name
    if not labels:
        return [Check("-", "e2e", "endpoints", SKIP, "no site uses e2e")]

    bad = set()
    for label in sorted(labels):
        ep = config.e2e.endpoint(label)
        try:
            transfer.call(f"get_endpoint {label}", lambda tc: tc.get_endpoint(ep.id))
            checks.append(Check("-", "e2e", f"endpoint {label}", OK, f"get_endpoint {ep.id}"))
        except LoginRequired as exc:
            checks.append(Check("-", "e2e", f"endpoint {label}", FAIL, str(exc.args[0]), exc.fix or fix))
            bad.add(label)
        except GlobusIriError as exc:
            checks.append(Check("-", "e2e", f"endpoint {label}", FAIL, str(exc)))
            bad.add(label)

    for (label, path), site_name in sorted(targets.items()):
        if label in bad:
            continue
        ep = config.e2e.endpoint(label)
        try:
            transfer.tc.operation_ls(ep.id, path=path, limit=1)
            checks.append(Check(site_name, "e2e", f"ls {label}", OK, f"ls {path}"))
        except globus_sdk.GlobusAPIError as exc:
            scopes = consent_required(exc)
            if scopes is not None:
                checks.append(Check(site_name, "e2e", f"ls {label}", FAIL, f"ConsentRequired data_access ({path})", fix))
            elif exc.http_status == 404 or "NotFound" in str(getattr(exc, "code", "")):
                checks.append(Check(site_name, "e2e", f"ls {label}", FAIL,
                                    f"{path} not found (check e2e paths and endpoints.{label}.path_map)"))
            else:
                checks.append(_e2e_ls_failure(site_name, label, path, exc, ep))
        except LoginRequired as exc:
            checks.append(Check(site_name, "e2e", f"ls {label}", FAIL, str(exc.args[0]), exc.fix or fix))
        except (GlobusIriError, globus_sdk.NetworkError) as exc:
            checks.append(Check(site_name, "e2e", f"ls {label}", FAIL, str(exc)))
    return checks


def _required_domains(exc):
    """Identity domains Globus says the session needs (authorization_parameters), if any."""
    try:
        params = exc.info.authorization_parameters
        return list(params.session_required_single_domain or []) if params else []
    except AttributeError:
        return []


def _e2e_ls_failure(site_name, label, path, exc, ep):
    code = str(getattr(exc, "code", "") or "")
    message = str(getattr(exc, "message", "") or "").strip().splitlines()
    detail = f"ls {path}: HTTP {exc.http_status} {code}" + (f": {message[0][:200]}" if message else "")
    domains = _required_domains(exc)
    fix = None
    if domains:
        fix = (f"set e2e.endpoints.{label}.session_domains: [{', '.join(domains)}], "
               "then globusiri e2e login --force --prompt-login")
    elif "LoginFailed" in code or "Authentication" in code:
        fix = (f"the collection rejected the token's identity; "
               + (f"set e2e.endpoints.{label}.session_domains to the domain of your {label} identity, then "
                  if not ep.session_domains else "check that you log in with the right identity, then ")
               + "globusiri e2e login --force --prompt-login")
    return Check(site_name, "e2e", f"ls {label}", FAIL, detail, fix)


# ---------------------------------------------------------------------- PanDA
def panda_check(mgr, site_name):
    try:
        remote = panda_secrets.get_secret(mgr.panda_secret_key)
    except PandaError as exc:
        return Check(site_name, mgr.kind, "panda secret", FAIL, str(exc))
    local = mgr.expires_at()
    remote_exp = remote.get("expires_at")
    try:
        remote_exp = float(remote_exp) if remote_exp else None
    except (TypeError, ValueError):
        remote_exp = None
    if remote_exp is None:
        return Check(site_name, mgr.kind, "panda secret", WARN, f"{mgr.panda_secret_key} has no expires_at (old format)",
                     mgr.fix("upload_to_panda"))
    if local is not None and abs(remote_exp - local) > 1:
        newer = "PanDA" if remote_exp > local else "local file"
        fix = mgr.fix("download_from_panda") if newer == "PanDA" else mgr.fix("upload_to_panda")
        return Check(site_name, mgr.kind, "panda secret", WARN, f"{mgr.panda_secret_key} out of sync ({newer} is newer)", fix)
    return Check(site_name, mgr.kind, "panda secret", OK, f"{mgr.panda_secret_key} in sync")


# ---------------------------------------------------------------------- driver
def validate(config, *, sites=None, kinds=("iri", "https", "e2e"), job=None, offline=False, write_test=False,
             panda=False, min_lifetime=0):
    """Run the checks and return a list of Check."""
    checks = []
    kinds = list(kinds)
    if job is not None:
        site = config.site(job.site)
        sites = [site]
        need = job.required_kinds(site)
        for kind, reason in job.skipped_kinds(site).items():
            if kind in kinds:
                checks.append(Check(site.name, kind, "-", SKIP, reason))
        kinds = [k for k in kinds if k in need]
    elif sites is None:
        sites = list(config.sites.values())

    if "iri" in kinds:
        seen_facility_files = set()
        for site in sites:
            mgr = IriTokenManager(config, site)
            fc = file_checks(mgr, site.name, min_lifetime)
            checks += fc
            usable = not any(c.status == FAIL and c.check == "token file" for c in fc)
            if panda and mgr.panda_secret_key and mgr.access_file not in seen_facility_files:
                checks.append(panda_check(mgr, site.name))
            seen_facility_files.add(mgr.access_file)
            if not offline and usable:
                api = iri_api_check(config, site)
                checks.append(api)
                if api.status != FAIL:
                    checks.append(iri_workdir_check(config, site))

    if "https" in kinds:
        for site in sites:
            if not site.has_https:
                checks.append(Check(site.name, "https", "-", SKIP, "site has no https section"))
                continue
            mgr = HttpsTokenManager(config, site)
            fc = file_checks(mgr, site.name, min_lifetime)
            checks += fc
            if panda and mgr.panda_secret_key:
                checks.append(panda_check(mgr, site.name))
            if not offline and not any(c.status == FAIL for c in fc):
                checks += https_checks(config, site, write_test)

    if "e2e" in kinds:
        e2e_sites = [s for s in sites if s.has_e2e]
        if not e2e_sites:
            checks.append(Check("-", "e2e", "-", SKIP, "no selected site uses e2e"))
        else:
            mgr = E2ETokenManager(config)
            fc = file_checks(mgr, "-", min_lifetime)
            checks += fc
            if panda and mgr.panda_secret_key:
                checks.append(panda_check(mgr, "-"))
            if not offline and not any(c.status == FAIL for c in fc):
                checks += e2e_checks(config, e2e_sites)
    return checks


def failed(checks, strict=False):
    bad = (FAIL, WARN) if strict else (FAIL,)
    return any(c.status in bad for c in checks)
