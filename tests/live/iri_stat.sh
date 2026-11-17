#!/bin/bash
# Stat a path on NERSC through the IRI filesystem API with plain curl (no globusiri needed).
#
#   tests/live/iri_stat.sh [PATH]
#
# Token: $IRI_TOKEN, else access_token from <WORK_DIR>/tokens/iri/nersc.access.yaml
#        (refresh it first with: globusiri iri refresh NERSC_Perlmutter).
# Override with $IRI_BASE_URL / $IRI_RESOURCE_ID (defaults: NERSC Perlmutter, as in sites.yaml).
# If stat returns 403/501, the same request is retried with ls (as `globusiri validate` does).
set -euo pipefail

TARGET=${1:-/global/cfs/cdirs/m2616/www/PanDA_logs/IRI/20261117-092123-f34eb7}
BASE_URL=${IRI_BASE_URL:-https://api.iri.nersc.gov}
RESOURCE_ID=${IRI_RESOURCE_ID:-59e80c79-4dfd-4c53-9c07-7405685fcd37}

if [[ -z ${IRI_TOKEN:-} ]]; then
    work_dir=${GLOBUSIRI_WORK_DIR:-$(globusiri paths 2>/dev/null | awk '$1 == "WORK_DIR" {print $2}')}
    access=${work_dir:?set GLOBUSIRI_WORK_DIR or IRI_TOKEN}/tokens/iri/nersc.access.yaml
    [[ -r $access ]] || { echo "no token file $access; run: globusiri iri refresh NERSC_Perlmutter" >&2; exit 1; }
    IRI_TOKEN=$(sed -n -E "s/^access_token:[[:space:]]*['\"]?([^'\"]+)['\"]?[[:space:]]*$/\1/p" "$access")
    [[ -n $IRI_TOKEN ]] || { echo "no access_token in $access" >&2; exit 1; }
fi

# GET <base>/api/v1/filesystem/<op>/<resource_id>?path=<urlencoded path>
call() {
    local op=$1
    echo ">>> GET $BASE_URL/api/v1/filesystem/$op/$RESOURCE_ID?path=$TARGET" >&2
    curl -sS -G -w '\n%{http_code}' \
        -H "Authorization: Bearer $IRI_TOKEN" \
        -H "Accept: application/json" \
        --data-urlencode "path=$TARGET" \
        "$BASE_URL/api/v1/filesystem/$op/$RESOURCE_ID"
}

pretty() { if command -v python3 >/dev/null; then python3 -m json.tool 2>/dev/null || cat; else cat; fi; }

out=$(call stat)
code=${out##*$'\n'}
body=${out%$'\n'*}
echo "HTTP $code"
printf '%s\n' "$body" | pretty

if [[ $code == 403 || $code == 501 ]]; then
    echo "stat failed with $code; trying ls" >&2
    out=$(call ls)
    code=${out##*$'\n'}
    body=${out%$'\n'*}
    echo "HTTP $code"
    printf '%s\n' "$body" | pretty
fi

[[ $code == 2?? ]]
