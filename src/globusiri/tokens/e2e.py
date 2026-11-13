"""E2E token: one Globus Transfer token shared by all sites.

The scope is transfer:all with optional dependent data_access/https scopes for every
endpoint in e2e.endpoints that has data_access: true.

Refresh file  tokens/e2e/refresh.yaml - client_id, refresh_token, <label>_id, <label>_https
Access file   tokens/e2e/access.yaml  - client_id, access_token, expires_at, <label>_https
              (the format go_bulk_preparator_v4 / go_bulk_stager_v4 read)
"""

import globus_sdk
from globus_sdk.scopes import GCSCollectionScopes, TransferScopes

from ..errors import ConfigError, LoginRequired
from ..secure_io import read_yaml
from .base import TokenManager, expires_at_from, interactive_login, require_sessions
from .https import search_collection, transfer_login


def build_transfer_scope(endpoint_ids):
    scope = TransferScopes.all
    for eid in endpoint_ids:
        coll = GCSCollectionScopes(eid)
        scope = scope.with_dependency(coll.data_access.with_optional(True))
        scope = scope.with_dependency(coll.https.with_optional(True))
    return scope


class E2ETokenManager(TokenManager):
    kind = "e2e"
    name = "-"

    def __init__(self, config):
        super().__init__(config)
        self.access_file = config.token_file("e2e.access", self.paths.e2e_access)
        self.refresh_file = config.token_file("e2e.refresh", self.paths.e2e_refresh)
        self.panda_secret_key = config.e2e.panda_secret_key
        self.client_id = config.globus_client_id

    def resolve_endpoints(self, tc):
        """Resolve each endpoint's id (by query if needed) and https_server.

        Fails if an endpoint has both id and query and they disagree.
        """
        resolved = {}
        for label, ep in self.config.e2e.endpoints.items():
            eid = ep.id
            if ep.query:
                found, candidates = search_collection(tc, ep.query, ep.pick)
                print(f"Candidates for '{ep.query}' ({label}):")
                for i, (cid, name) in enumerate(candidates):
                    print(f"  {'>>>' if cid == found else '   '} [{i}] {name} ({cid})")
                if eid and eid != found:
                    raise ConfigError(
                        f"e2e.endpoints.{label}: query '{ep.query}' pick {ep.pick} resolved to {found}, "
                        f"but id is {eid}; fix 'pick' or 'id'"
                    )
                eid = found
            https_server = tc.get_endpoint(eid).get("https_server")
            resolved[label] = {"id": eid, "https_server": https_server, "data_access": ep.data_access,
                               "session_domains": ep.session_domains}
        if not resolved:
            raise ConfigError("e2e.endpoints is empty")
        return resolved

    def login(self, *, force=False, prompt_login=False, import_file=None):
        if self.refresh_file.is_file() and not force and not import_file:
            raise LoginRequired(f"{self.refresh_file} already exists; pass --force to log in again")
        if import_file:
            data = read_yaml(import_file)
            if not data.get("client_id") or not data.get("refresh_token"):
                raise LoginRequired(f"{import_file} is missing client_id or refresh_token")
            self.write_refresh(data)
            return self._do_refresh()

        tc = transfer_login(self.client_id, prompt_login)
        endpoints = self.resolve_endpoints(tc)
        domains = [d for e in endpoints.values() for d in e["session_domains"]]
        unset = [label for label, e in endpoints.items() if e["data_access"] and not e["session_domains"]]
        if unset:
            print(f"Note: no session_domains for {', '.join(unset)} in {self.config.path or "sites.yaml"}; Globus will not ask "
                  "you to log in to those sites. If a collection then fails with LoginFailed or "
                  "AuthenticationRequired, set e2e.endpoints.<LABEL>.session_domains and log in again.")
        require_sessions(self.client_id, domains, what="e2e")
        scope = build_transfer_scope([e["id"] for e in endpoints.values() if e["data_access"]])
        print(f"\nRequesting scope:\n{scope}\n")
        # prompt_login here would drop the session identities gathered above
        resp = interactive_login(self.client_id, [scope], refresh_tokens=True,
                                 prompt_login=prompt_login and not domains,
                                 message="Final step: Globus Transfer login with data_access consents.")
        tok = resp.by_resource_server.get(TransferScopes.resource_server) or {}
        if not tok.get("refresh_token"):
            raise LoginRequired("login returned no Transfer refresh token")
        data = {"client_id": self.client_id, "refresh_token": tok["refresh_token"]}
        for label, e in endpoints.items():
            data[f"{label}_id"] = e["id"]
            data[f"{label}_https"] = e["https_server"]
        self.write_refresh(data)
        return self._do_refresh()

    def _do_refresh(self):
        data = self.read_refresh()
        client_id, refresh_token = data.get("client_id"), data.get("refresh_token")
        if not (client_id and refresh_token):
            raise LoginRequired(f"{self.refresh_file} is missing client_id or refresh_token", fix=self.fix())
        try:
            resp = globus_sdk.NativeAppAuthClient(client_id).oauth2_refresh_token(refresh_token)
        except globus_sdk.GlobusAPIError as exc:
            raise LoginRequired(f"Transfer token refresh failed: HTTP {exc.http_status}: {exc.message}", fix=self.fix()) from exc
        tok = resp.by_resource_server.get(TransferScopes.resource_server) or {}
        if not tok.get("access_token"):
            raise LoginRequired("refresh returned no Transfer access token", fix=self.fix())
        if tok.get("refresh_token") and tok["refresh_token"] != refresh_token:
            data["refresh_token"] = tok["refresh_token"]
            self.write_refresh(data)
        expires_at = expires_at_from(tok)
        access = {"client_id": client_id, "access_token": tok["access_token"], "expires_at": expires_at}
        access.update({k: v for k, v in data.items() if k.endswith("_https")})
        self.write_access(access)
        return expires_at

    def panda_payload(self):
        data = self.read_access()
        payload = dict(data)
        # also in the layout the old generate_transfer_token_to_panda.py --get_access_token reads
        payload["endpoints"] = {k[: -len("_https")]: v for k, v in data.items() if k.endswith("_https")}
        return payload

    def access_from_panda(self, payload):
        out = {k: payload.get(k) for k in ("client_id", "access_token", "expires_at")}
        for k, v in payload.items():
            if k.endswith("_https"):
                out[k] = v
        for label, server in (payload.get("endpoints") or {}).items():
            out.setdefault(f"{label}_https", server)
        return out
