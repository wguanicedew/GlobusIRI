"""Common token-manager behaviour: refresh skipping, PanDA sync, status."""

import json
import time
from dataclasses import dataclass, field

import globus_sdk

from .. import panda_secrets
from ..errors import LoginRequired
from ..secure_io import is_private, read_json, read_yaml, write_json_private, write_yaml_private


@dataclass
class TokenStatus:
    kind: str
    name: str
    access_file: str
    access_exists: bool
    access_private: bool = True
    expires_at: float = None
    refresh_file: str = None
    refresh_exists: bool = False
    extra: dict = field(default_factory=dict)

    @property
    def remaining(self):
        return None if self.expires_at is None else self.expires_at - time.time()


@dataclass
class RefreshResult:
    refreshed: bool
    expires_at: float = None
    reason: str = ""
    uploaded: bool = False


def prompt_code(url, message=None):
    """Print the consent URL and read the auth code from stdin."""
    if message:
        print(message)
    print(f"\nPlease open this URL, log in, and consent:\n{url}\n")
    code = input("Enter the authorization code: ").strip()
    if not code:
        raise LoginRequired("no authorization code entered")
    return code


def interactive_login(client_id, scopes, *, refresh_tokens=False, prompt_login=False, message=None):
    """Run a native-app login flow and return the OAuthTokenResponse."""
    client = globus_sdk.NativeAppAuthClient(client_id)
    client.oauth2_start_flow(requested_scopes=scopes, refresh_tokens=refresh_tokens)
    url = client.oauth2_get_authorize_url(prompt="login" if prompt_login else globus_sdk.MISSING)
    code = prompt_code(url, message)
    try:
        return client.oauth2_exchange_code_for_tokens(code)
    except globus_sdk.GlobusAPIError as exc:
        raise LoginRequired(f"code exchange failed: HTTP {exc.http_status}: {exc.message}") from exc


def require_sessions(client_id, domains, *, what=None, prompt_login=False):
    """Make the user log in with an identity from each domain, one by one, in the same
    browser session. Globus adds each identity to the session, so a later token request
    is issued with all of them present. A scope alone only asks for consent; it never
    makes Globus ask for a login with a particular identity. With prompt_login, the first
    step forces a fresh login (the following steps add to that session)."""
    domains = list(dict.fromkeys(domains or []))
    if not domains:
        return
    client = globus_sdk.NativeAppAuthClient(client_id)
    for i, domain in enumerate(domains, 1):
        # a new flow per step: Globus rejects a reused PKCE code_challenge
        client.oauth2_start_flow(requested_scopes=["openid"])
        url = client.oauth2_get_authorize_url(
            session_required_single_domain=domain,
            session_message=f"globusiri{' ' + what if what else ''}: log in with your {domain} identity",
            prompt="login" if prompt_login and i == 1 else globus_sdk.MISSING,
        )
        print(f"\nSession login {i}/{len(domains)}: log in with your {domain} identity"
              f"{' (' + what + ')' if what else ''}.")
        print(f"Open this URL in the same browser for every step:\n{url}\n")
        input("Press Enter after logging in (the authorization code is not needed): ")


class TokenManager:
    """Base class. Subclasses implement login() and _do_refresh()."""

    kind = None

    def __init__(self, config):
        self.config = config
        self.paths = config.paths

    # subclasses provide these
    name = None
    access_file = None
    refresh_file = None
    panda_secret_key = None

    def fix(self, action="login"):
        target = f" {self.name}" if self.kind != "e2e" else ""
        return f"globusiri {self.kind} {action}{target}"

    # ------------------------------------------------------------------ files
    def read_access(self):
        if not self.access_file.is_file():
            raise LoginRequired(f"{self.kind} access token file {self.access_file} not found", fix=self.fix("refresh"))
        return read_yaml(self.access_file)

    def write_access(self, data):
        write_yaml_private(self.access_file, data)

    def read_refresh(self):
        if not self.refresh_file.is_file():
            raise LoginRequired(f"{self.kind} refresh token file {self.refresh_file} not found", fix=self.fix())
        if self.refresh_file.suffix == ".json":
            return read_json(self.refresh_file)
        return read_yaml(self.refresh_file)

    def write_refresh(self, data):
        if self.refresh_file.suffix == ".json":
            write_json_private(self.refresh_file, data)
        else:
            write_yaml_private(self.refresh_file, data)

    def expires_at(self):
        try:
            exp = self.read_access().get("expires_at")
            return float(exp) if exp else None
        except (LoginRequired, TypeError, ValueError):
            return None

    # ------------------------------------------------------------------ ops
    def refresh(self, *, min_lifetime=0, upload_to_panda=False, validate=None):
        """Refresh unless the current token outlives min_lifetime. Never prompts."""
        exp = self.expires_at()
        if min_lifetime and exp and exp - time.time() > min_lifetime:
            result = RefreshResult(False, exp, f"still valid for {int((exp - time.time()) / 3600)}h")
        else:
            exp = self._do_refresh()
            result = RefreshResult(True, exp, "refreshed")
            if validate is None:
                validate = self.validate_after_refresh()
            if validate:
                self.validate_live()
        if upload_to_panda and self._panda_needs_upload(result.expires_at):
            self.upload_to_panda()
            result.uploaded = True
        return result

    def validate_after_refresh(self):
        return False

    def validate_live(self):
        """Hook for refresh-time validation; raises on failure."""

    def panda_payload(self):
        return self.read_access()

    def upload_to_panda(self):
        payload = self.panda_payload()
        panda_secrets.set_secret(self.panda_secret_key, payload)
        self._record_panda_sync(payload.get("expires_at"))

    def download_from_panda(self):
        payload = panda_secrets.get_secret(self.panda_secret_key)
        data = self.access_from_panda(payload)
        if not data.get("access_token"):
            raise LoginRequired(f"PanDA secret {self.panda_secret_key} has no access_token")
        self.write_access(data)
        return data

    def access_from_panda(self, payload):
        return dict(payload)

    def status(self):
        st = TokenStatus(
            kind=self.kind,
            name=self.name,
            access_file=str(self.access_file),
            access_exists=self.access_file.is_file(),
            refresh_file=str(self.refresh_file),
            refresh_exists=self.refresh_file.is_file(),
        )
        if st.access_exists:
            st.access_private = is_private(self.access_file)
            st.expires_at = self.expires_at()
        return st

    # ------------------------------------------------------------------ PanDA sync bookkeeping
    def _sync_key(self):
        return f"{self.kind}:{self.name}"

    def _read_sync(self):
        try:
            return json.loads(self.paths.panda_sync.read_text())
        except (OSError, ValueError):
            return {}

    def _record_panda_sync(self, expires_at):
        data = self._read_sync()
        data[self._sync_key()] = expires_at
        write_json_private(self.paths.panda_sync, data)

    def _panda_needs_upload(self, expires_at):
        return self._read_sync().get(self._sync_key()) != expires_at


def expires_at_from(token_data):
    """expires_at (unix seconds) from a token dict holding expires_at_seconds or expires_in."""
    if token_data.get("expires_at_seconds"):
        return int(token_data["expires_at_seconds"])
    if token_data.get("expires_in"):
        return int(time.time() + int(token_data["expires_in"]))
    return None
