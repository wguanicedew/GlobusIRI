#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Token managers for the three independent token kinds: iri, https, e2e."""

from .base import TokenStatus
from .e2e import E2ETokenManager
from .https import HttpsTokenManager
from .iri import IriTokenManager

KINDS = ("iri", "https", "e2e")


def manager(kind, config, site=None):
    if kind == "iri":
        return IriTokenManager(config, site)
    if kind == "https":
        return HttpsTokenManager(config, site)
    if kind == "e2e":
        return E2ETokenManager(config)
    raise ValueError(f"unknown token kind '{kind}'")


__all__ = ["KINDS", "TokenStatus", "manager", "IriTokenManager", "HttpsTokenManager", "E2ETokenManager"]
