#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""HTTPS tokens: one per site, scoped to that site's Globus HTTPS collection.

The collection is discovered at login (endpoint_search on https.collection_query, as
generate_https_token_to_panda.py does) unless https.collection_id is pinned. Its
collection_id and https_server are stored in the token files and read from there.

Refresh file  tokens/https/<SITE>.refresh.yaml - client_id, refresh_token, collection_id, https_server
Access file   tokens/https/<SITE>.access.yaml  - client_id, access_token, expires_at, collection_id, https_server
"""

import globus_sdk
from globus_sdk.scopes import GCSCollectionScopes, TransferScopes

from ..errors import ConfigError, LoginRequired
from ..secure_io import read_yaml
from .base import TokenManager, expires_at_from, interactive_login, require_sessions


def search_collection(tc, query, pick=0):
    """endpoint_search(query) -> (collection_id, candidates)."""
    results = list(tc.endpoint_search(filter_fulltext=query, limit=5))
    if not results:
        raise ConfigError(f"no Globus collection found for query '{query}'")
    if pick >= len(results):
        raise ConfigError(f"query '{query}' returned {len(results)} results; pick={pick} is out of range")
    candidates = [(r["id"], r.get("display_name")) for r in results]
    return results[pick]["id"], candidates


def transfer_login(client_id, prompt_login=False):
    resp = interactive_login(client_id, [TransferScopes.all], prompt_login=prompt_login,
                             message="Step 1: basic Globus Transfer login (used only to look up collections).")
    token = resp.by_resource_server[TransferScopes.resource_server]["access_token"]
    return globus_sdk.TransferClient(authorizer=globus_sdk.AccessTokenAuthorizer(token))


class HttpsTokenManager(TokenManager):
    kind = "https"

    def __init__(self, config, site):
        super().__init__(config)
        if site is None:
            raise ConfigError("an HTTPS token needs a SITE")
        if not site.has_https:
            raise ConfigError(f"site {site.name} has no 'https' section")
        self.site = site
        self.name = site.name
        self.access_file = config.token_file(f"https.{site.name}.access", self.paths.https_access(site.name))
        self.refresh_file = config.token_file(f"https.{site.name}.refresh", self.paths.https_refresh(site.name))
        self.panda_secret_key = site.https_panda_secret_key
        self.client_id = config.globus_client_id

    def login(self, *, force=False, prompt_login=False, import_file=None):
        if self.refresh_file.is_file() and not force and not import_file:
            raise LoginRequired(f"{self.refresh_file} already exists; pass --force to log in again")
        if import_file:
            data = read_yaml(import_file)
            missing = [k for k in ("client_id", "refresh_token", "collection_id") if not data.get(k)]
            if missing:
                raise LoginRequired(f"{import_file} is missing {', '.join(missing)}")
            self.write_refresh(data)
            return self._do_refresh()

        collection_id = self.site.https_collection_id
        https_server = self.site.https_server
        pinned = bool(collection_id and https_server)  # no step-1 search login
        if not pinned:
            tc = transfer_login(self.client_id, prompt_login)
            if not collection_id:
                if not self.site.https_collection_query:
                    raise ConfigError(f"sites.{self.name}.https needs collection_query or collection_id")
                collection_id, candidates = search_collection(tc, self.site.https_collection_query, self.site.https_collection_pick)
                print(f"Candidates for '{self.site.https_collection_query}':")
                for i, (cid, name) in enumerate(candidates):
                    print(f"  {'>>>' if cid == collection_id else '   '} [{i}] {name} ({cid})")
            https_server = https_server or tc.get_endpoint(collection_id).get("https_server")
            if not https_server:
                raise ConfigError(f"collection {collection_id} has no https_server")

        domains = self.site.https_session_domains
        # step 1 already forced a fresh login unless it was skipped
        require_sessions(self.client_id, domains, what=f"https {self.name}", prompt_login=prompt_login and pinned)
        coll = GCSCollectionScopes(collection_id)
        resp = interactive_login(
            self.client_id,
            [coll.https, coll.data_access],
            refresh_tokens=True,
            prompt_login=prompt_login and not domains,
            message=(
                f"Step 2: log in with the identity that has access to {self.name}.\n"
                "If your Globus session is logged in as a different site's identity, log out first "
                "(https://auth.globus.org/logout)."
            ),
        )
        tok = resp.by_resource_server.get(collection_id) or {}
        if not tok.get("refresh_token"):
            raise LoginRequired("login returned no refresh token for the collection")
        self.write_refresh({
            "client_id": self.client_id,
            "refresh_token": tok["refresh_token"],
            "collection_id": collection_id,
            "https_server": https_server,
        })
        return self._do_refresh()

    def _do_refresh(self):
        data = self.read_refresh()
        client_id, refresh_token, cid = data.get("client_id"), data.get("refresh_token"), data.get("collection_id")
        if not (client_id and refresh_token and cid):
            raise LoginRequired(f"{self.refresh_file} is missing client_id, refresh_token or collection_id", fix=self.fix())
        try:
            resp = globus_sdk.NativeAppAuthClient(client_id).oauth2_refresh_token(refresh_token)
        except globus_sdk.GlobusAPIError as exc:
            raise LoginRequired(f"HTTPS token refresh failed: HTTP {exc.http_status}: {exc.message}", fix=self.fix()) from exc
        tok = resp.by_resource_server.get(cid) or {}
        if not tok.get("access_token"):
            raise LoginRequired(f"refresh returned no access token for collection {cid}", fix=self.fix())
        if tok.get("refresh_token") and tok["refresh_token"] != refresh_token:
            data["refresh_token"] = tok["refresh_token"]
            self.write_refresh(data)
        expires_at = expires_at_from(tok)
        self.write_access({
            "client_id": client_id,
            "access_token": tok["access_token"],
            "expires_at": expires_at,
            "collection_id": cid,
            "https_server": data.get("https_server"),
        })
        return expires_at

    def access_from_panda(self, payload):
        keys = ("client_id", "access_token", "expires_at", "collection_id", "https_server")
        return {k: payload.get(k) for k in keys}
