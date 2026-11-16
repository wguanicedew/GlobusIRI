#!/bin/bash
# Live E2E test (real Globus transfers with your e2e token; not run by pytest).
#
#   1. source: Perlmutter  /global/cfs/cdirs/m2616/harvester_workdir/pilot_env/perlmutter_iri_main.sh
#   2. Perlmutter -> SDCC  as SCOPE:NAME (default user.wguan:fileA) at its Rucio deterministic
#                          path below /dcint-door003/usatlas.bnl.gov/atlasdatadisk/rucio
#   3. SDCC -> Perlmutter  /global/cfs/cdirs/m2616/harvester_workdir/rucio/<rucio path>
#      SDCC -> Crux        /ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio/<rucio path>
#
# Usage: tests/live/e2e_rucio_chain.sh [SCOPE] [NAME]
# Needs: globusiri e2e login (BNL, NERSC and ALCF identities), and sites.yaml with
# NERSC_Perlmutter / ALCF_CRUX e2e sections (stagein src_path = the SDCC rucio dir, base_path set).
# Files are only placed by path: nothing is registered in Rucio.
set -euo pipefail

SCOPE=${1:-user.wguan}
NAME=${2:-fileA}
SRC=/global/cfs/cdirs/m2616/harvester_workdir/pilot_env/perlmutter_iri_main.sh
SDCC_RUCIO=/dcint-door003/usatlas.bnl.gov/atlasdatadisk/rucio

echo "== 1+2. Perlmutter -> SDCC as $SCOPE:$NAME (Rucio deterministic path under $SDCC_RUCIO)"
globusiri e2e stageout NERSC_Perlmutter "$SRC" "$SDCC_RUCIO" \
    --rucio-deterministic --scope "$SCOPE" --name "$NAME" --dst-endpoint BNL_dCache

echo "== 3a. SDCC -> Perlmutter rucio dir"
globusiri e2e stagein NERSC_Perlmutter --rucio "$SCOPE:$NAME" --src-endpoint BNL_dCache --sync-level exists

echo "== 3b. SDCC -> Crux rucio dir"
globusiri e2e stagein ALCF_CRUX --rucio "$SCOPE:$NAME" --src-endpoint BNL_dCache --sync-level exists

echo "== done. Remove the HPC copies with:"
echo "   globusiri e2e cleanup NERSC_Perlmutter --rucio $SCOPE:$NAME"
echo "   globusiri e2e cleanup ALCF_CRUX --rucio $SCOPE:$NAME"
