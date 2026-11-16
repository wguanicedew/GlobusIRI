#!/bin/bash
# Generic GlobusIRI job script: runs template_vars.command in the run directory.
# Resource requests, stdout and stderr are supplied through the IRI job spec.
set -euo pipefail
cd "{work_dir}"

# The job file's resources, for commands that start several processes (srun / mpiexec).
export GLOBUSIRI_RUN_ID="{runID}"
export GLOBUSIRI_NNODE={nNode}
export GLOBUSIRI_NPROC_PER_NODE={nProcessPerNode}
export GLOBUSIRI_NPROC=$(({nNode} * {nProcessPerNode}))
export GLOBUSIRI_NCORE_PER_PROC={nCorePerProcess}

# ---- input stage-in: rank 0 extracts the input archive, the other ranks wait ----
# The script may run once, or once per rank (NERSC's IRI starts it under srun per rank);
# the rank comes from the launcher's environment and is 0 if none is set. Other ranks give
# up after $GLOBUSIRI_STAGEIN_TIMEOUT seconds (default 1800).
GLOBUSIRI_ARCHIVE="{inputArchive}"
if [ -n "$GLOBUSIRI_ARCHIVE" ]; then
    cd "{work_dir}" || exit 1
    GLOBUSIRI_RANK="${SLURM_PROCID-}"
    [ -n "$GLOBUSIRI_RANK" ] || GLOBUSIRI_RANK="${PMIX_RANK-}"
    [ -n "$GLOBUSIRI_RANK" ] || GLOBUSIRI_RANK="${PMI_RANK-}"
    [ -n "$GLOBUSIRI_RANK" ] || GLOBUSIRI_RANK="${PALS_RANKID-}"
    [ -n "$GLOBUSIRI_RANK" ] || GLOBUSIRI_RANK="${OMPI_COMM_WORLD_RANK-}"
    [ -n "$GLOBUSIRI_RANK" ] || GLOBUSIRI_RANK=0
    if [ "$GLOBUSIRI_RANK" = "0" ]; then
        if [ ! -f .globusiri_inputs.done ]; then
            rm -f .globusiri_inputs.failed
            echo "[globusiri] rank 0: extracting $GLOBUSIRI_ARCHIVE"
            if tar -xzf "$GLOBUSIRI_ARCHIVE"; then
                touch .globusiri_inputs.done
            else
                touch .globusiri_inputs.failed
                echo "[globusiri] rank 0: failed to extract $GLOBUSIRI_ARCHIVE" >&2
                exit 1
            fi
        fi
    else
        GLOBUSIRI_WAITED=0
        while [ ! -f .globusiri_inputs.done ]; do
            if [ -f .globusiri_inputs.failed ]; then
                echo "[globusiri] rank $GLOBUSIRI_RANK: rank 0 failed to extract $GLOBUSIRI_ARCHIVE" >&2
                exit 1
            fi
            if [ "$GLOBUSIRI_WAITED" -ge "${GLOBUSIRI_STAGEIN_TIMEOUT-1800}" ]; then
                echo "[globusiri] rank $GLOBUSIRI_RANK: timed out waiting for $GLOBUSIRI_ARCHIVE" >&2
                exit 1
            fi
            sleep 2
            GLOBUSIRI_WAITED=$((GLOBUSIRI_WAITED + 2))
        done
    fi
fi
# ---- end input stage-in ----

{command}
