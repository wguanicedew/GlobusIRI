#!/bin/bash
# Resource requests, stdout and stderr are supplied through the IRI job spec.
set -euo pipefail
cd "{work_dir}"
{command}
