#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Build clients for a site from the conventional token files."""

from .clients.http import TokenSource
from .clients.https import GlobusHttpsClient
from .clients.iri import IriClient
from .clients.transfer import TransferWrapper
from .tokens import E2ETokenManager, HttpsTokenManager, IriTokenManager


def iri_client(config, site, *, debug=None):
    mgr = IriTokenManager(config, site)
    source = TokenSource(mgr.access_file, label=f"IRI ({site.facility})", fix=mgr.fix("refresh"))
    return IriClient(site.base_url, source, resource_id=site.resource_id, api_version=site.iri_api_version,
                     retries=config.retries,
                     sfapi_retries=site.iri_sfapi_retries, sfapi_retry_sleep=site.iri_sfapi_retry_sleep,
                     retry_attempts=site.iri_retries, retry_sleep=site.iri_retry_sleep,
                     debug=site.iri_debug if debug is None else debug)


def https_client(config, site, *, debug=None):
    mgr = HttpsTokenManager(config, site)
    source = TokenSource(mgr.access_file, label=f"HTTPS ({site.name})", fix=mgr.fix("refresh"))
    return GlobusHttpsClient(source, site.https_path_map, retries=config.retries,
                             retry_attempts=site.https_retries, retry_sleep=site.https_retry_sleep,
                             debug=site.iri_debug if debug is None else debug, https_server=site.https_server)


def transfer(config):
    mgr = E2ETokenManager(config)
    return TransferWrapper(TokenSource(mgr.access_file, label="E2E Transfer", fix=mgr.fix("refresh")))
