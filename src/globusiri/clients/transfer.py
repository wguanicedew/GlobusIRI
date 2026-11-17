#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Thin wrapper over globus_sdk.TransferClient (globus-sdk v4 only).

The client is rebuilt whenever the access token file changes, so a cron refresh is
picked up without restarting.
"""

import globus_sdk

from ..errors import LoginRequired, RemoteError


def consent_required(exc):
    """Return the required scopes if exc is a ConsentRequired error, else None."""
    if isinstance(exc, globus_sdk.GlobusAPIError):
        info = getattr(exc.info, "consent_required", None)
        if info:
            return list(info.required_scopes)
        if getattr(exc, "code", None) == "ConsentRequired":
            return []
    return None


class TransferWrapper:
    def __init__(self, token_source):
        self.token_source = token_source
        self._token = None
        self._tc = None

    @property
    def tc(self):
        token = self.token_source.token()
        if token != self._token or self._tc is None:
            self._tc = globus_sdk.TransferClient(authorizer=globus_sdk.AccessTokenAuthorizer(token))
            self._token = token
        return self._tc

    def call(self, what, fn, *args, **kwargs):
        """Run fn(tc, ...) and translate Globus errors into GlobusIRI errors."""
        try:
            return fn(self.tc, *args, **kwargs)
        except globus_sdk.GlobusAPIError as exc:
            scopes = consent_required(exc)
            if scopes is not None:
                raise LoginRequired(
                    f"{what}: ConsentRequired {' '.join(scopes)}".strip(),
                    fix="globusiri e2e login --force",
                ) from exc
            if exc.http_status in (401, 403) and getattr(exc, "code", "") in ("AuthenticationFailed", "TokenExpired"):
                raise LoginRequired(f"{what}: {exc.code}: {exc.message}", fix=self.token_source.fix) from exc
            raise RemoteError(f"{what}: HTTP {exc.http_status} {getattr(exc, 'code', '')}: {exc.message}",
                              status=exc.http_status) from exc
        except globus_sdk.NetworkError as exc:
            raise RemoteError(f"{what}: network error: {exc}") from exc
