#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""GlobusIRI: run HPC jobs through the IRI API and move files with Globus.

Python API (every CLI command is a method)::

    from globusiri import GlobusIRI
    g = GlobusIRI()
    g.job_submit("perlmutter_example_job.yaml")
"""

__version__ = "0.1.0"

__all__ = ["GlobusIRI", "__version__"]


def __getattr__(name):  # lazy, so `import globusiri` stays cheap and cycle-free
    if name == "GlobusIRI":
        from .api import GlobusIRI

        return GlobusIRI
    raise AttributeError(f"module 'globusiri' has no attribute {name!r}")
