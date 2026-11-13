"""IRI tokens: one per facility (nersc, alcf), from facility-specific Globus clients.

Refresh file  tokens/iri/<facility>.refresh.json  - full Globus token response
              (same layout as the old ~/.globus/<facility>_auth_tokens.json, so it can be imported)
Access file   tokens/iri/<facility>.access.yaml   - base_url, resource_id, access_token, expires_at
"""

import globus_sdk

from ..errors import ConfigError, LoginRequired
from ..secure_io import read_json
from .base import TokenManager, expires_at_from, interactive_login, require_sessions

NERSC_CLIENT_ID = "fae5c579-490a-4d76-b6eb-d78f65caeb63"
ALCF_CLIENT_ID = "8b84fc2d-49e9-49ea-b54d-b3a29a70cf31"
KNOWN_CLIENT_IDS = (NERSC_CLIENT_ID, ALCF_CLIENT_ID)

FACILITIES = {
    "nersc": {
        "client_id": NERSC_CLIENT_ID,
        "scope": "https://auth.globus.org/scopes/ed3e577d-f7f3-4639-b96e-ff5a8445d699/iri_api",
        "label": "NERSC IRI API",
    },
    "alcf": {
        "client_id": ALCF_CLIENT_ID,
        "scope": "https://auth.globus.org/scopes/6be511f6-a071-471f-9bc0-02a0d0836723/filesystem",
        "label": "ALCF IRI API",
    },
}
IDENTITY_SCOPES = ["openid", "profile", "email", "urn:globus:auth:scope:auth.globus.org:view_identities"]


def _scopes_of(token):
    return set((token.get("scope") or "").split())


def find_facility_token(stored, scope):
    """Return the token dict (top-level or in other_tokens) whose scope includes `scope`."""
    if scope in _scopes_of(stored):
        return stored
    for tok in stored.get("other_tokens") or []:
        if scope in _scopes_of(tok):
            return tok
    return None


class IriTokenManager(TokenManager):
    kind = "iri"

    def __init__(self, config, site):
        super().__init__(config)
        if site is None:
            raise ConfigError("an IRI token needs a SITE (it selects the facility)")
        self.site = site
        self.facility = site.facility
        self.fac = FACILITIES[self.facility]
        self.name = site.name
        self.access_file = config.token_file(f"iri.{self.facility}.access", self.paths.iri_access(self.facility))
        self.refresh_file = config.token_file(f"iri.{self.facility}.refresh", self.paths.iri_refresh(self.facility))
        self.panda_secret_key = site.iri_panda_secret_key

    def _sync_key(self):
        return f"iri:{self.facility}"

    # ------------------------------------------------------------------ login
    def login(self, *, force=False, prompt_login=False, import_file=None):
        if self.refresh_file.is_file() and not force and not import_file:
            raise LoginRequired(f"{self.refresh_file} already exists; pass --force to log in again")
        if import_file:
            stored = read_json(import_file)
            if not find_facility_token(stored, self.fac["scope"]):
                raise LoginRequired(f"{import_file} has no token for {self.fac['label']}")
        else:
            domains = self.site.iri_session_domains
            require_sessions(self.fac["client_id"], domains, what=f"iri {self.site.name}", prompt_login=prompt_login)
            resp = interactive_login(
                self.fac["client_id"],
                IDENTITY_SCOPES + [self.fac["scope"]],
                refresh_tokens=True,
                prompt_login=prompt_login and not domains,
                message=f"Logging in for {self.fac['label']} ({self.site.name}).",
            )
            stored = dict(resp.data)
            stored["client_id"] = self.fac["client_id"]
        self.write_refresh(stored)
        return self._do_refresh()

    # ------------------------------------------------------------------ refresh
    def _do_refresh(self):
        stored = self.read_refresh()
        scope = self.fac["scope"]
        tok = find_facility_token(stored, scope)
        if not tok or not tok.get("refresh_token"):
            raise LoginRequired(f"no refresh token for {self.fac['label']} in {self.refresh_file}", fix=self.fix())

        client_ids = [stored.get("client_id") or self.fac["client_id"]]
        client_ids += [c for c in KNOWN_CLIENT_IDS if c not in client_ids]
        failures = []
        new = None
        for cid in client_ids:
            try:
                new = dict(globus_sdk.NativeAppAuthClient(cid).oauth2_refresh_token(tok["refresh_token"]).data)
                stored["client_id"] = cid
                break
            except globus_sdk.GlobusAPIError as exc:
                failures.append(f"{cid[:8]}: HTTP {exc.http_status}")
            except globus_sdk.NetworkError as exc:
                failures.append(f"{cid[:8]}: {exc}")
        if new is None:
            raise LoginRequired(f"refresh failed for {self.fac['label']} ({'; '.join(failures)})", fix=self.fix())

        # the facility token may come back top-level or inside other_tokens
        fresh = {k: v for k, v in (find_facility_token(new, scope) or new).items() if k != "other_tokens"}
        # keep the old refresh token unless the server rotated it
        fresh.setdefault("refresh_token", None)
        fresh["refresh_token"] = fresh["refresh_token"] or tok["refresh_token"]
        # tok is the dict inside `stored` (top-level or an element of other_tokens): update in place
        tok.update(fresh)
        self.write_refresh(stored)

        expires_at = expires_at_from(fresh)
        self.write_access({
            "base_url": self.site.base_url,
            "resource_id": self.site.resource_id,
            "access_token": fresh["access_token"],
            "expires_at": expires_at,
        })
        return expires_at

    def validate_after_refresh(self):
        return self.site.iri_validate

    def validate_live(self):
        from ..validate import iri_api_check

        check = iri_api_check(self.config, self.site)
        if check.status == "FAIL":
            from ..errors import ValidationFailed

            raise ValidationFailed(f"IRI token validation failed: {check.detail}", fix=check.fix)

    # ------------------------------------------------------------------ PanDA
    def access_from_panda(self, payload):
        return {
            "base_url": payload.get("base_url") or self.site.base_url,
            "resource_id": payload.get("resource_id") or self.site.resource_id,
            "access_token": payload.get("access_token"),
            "expires_at": payload.get("expires_at"),
        }
