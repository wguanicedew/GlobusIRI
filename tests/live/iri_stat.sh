#!/bin/bash
# Stat a path on NERSC through the IRI filesystem API with plain curl (no globusiri needed).
#
#   tests/live/iri_stat.sh [PATH]
#
# Token: $IRI_TOKEN, else access_token from <WORK_DIR>/tokens/iri/nersc.access.yaml
#        (refresh it first with: globusiri iri refresh NERSC_Perlmutter).
# Override with $IRI_BASE_URL / $IRI_RESOURCE_ID (defaults: NERSC Perlmutter, as in sites.yaml).
#
# The filesystem calls are asynchronous: the reply is {"task_id": ..., "task_uri": ...}, and the
# result comes from polling GET task_uri until its status is completed, failed or canceled
# (every $TASK_POLL_INTERVAL s, at most $TASK_MAX_POLLS times; same as the globusiri client).
# If stat fails (HTTP 403/501, or its task fails), the same path is tried with ls, as
# `globusiri validate` does.
set -euo pipefail

TARGET=${1:-/global/cfs/cdirs/m2616/www/PanDA_logs/IRI/20261117-092123-f34eb7}
BASE_URL=${IRI_BASE_URL:-https://api.iri.nersc.gov}
RESOURCE_ID=${IRI_RESOURCE_ID:-59e80c79-4dfd-4c53-9c07-7405685fcd37}
TASK_POLL_INTERVAL=${TASK_POLL_INTERVAL:-5}
TASK_MAX_POLLS=${TASK_MAX_POLLS:-60}

command -v python3 >/dev/null || { echo "python3 is needed to read the JSON replies" >&2; exit 1; }

if [[ -z ${IRI_TOKEN:-} ]]; then
    work_dir=${GLOBUSIRI_WORK_DIR:-$(globusiri paths 2>/dev/null | awk '$1 == "WORK_DIR" {print $2}')}
    access=${work_dir:?set GLOBUSIRI_WORK_DIR or IRI_TOKEN}/tokens/iri/nersc.access.yaml
    [[ -r $access ]] || { echo "no token file $access; run: globusiri iri refresh NERSC_Perlmutter" >&2; exit 1; }
    IRI_TOKEN=$(sed -n -E "s/^access_token:[[:space:]]*['\"]?([^'\"]+)['\"]?[[:space:]]*$/\1/p" "$access")
    [[ -n $IRI_TOKEN ]] || { echo "no access_token in $access" >&2; exit 1; }
fi

# json_get KEY < json: the top-level value of KEY ("" if missing or not JSON)
json_get() {
    python3 -c '
import json, sys
try:
    v = json.load(sys.stdin).get(sys.argv[1], "")
except Exception:
    v = ""
print(v if isinstance(v, str) else json.dumps(v))' "$1"
}

pretty() { python3 -m json.tool 2>/dev/null || cat; }

# http GET URL [curl args...]: sets CODE and BODY
http() {
    local url=$1 out
    shift
    out=$(curl -sS -G -w '\n%{http_code}' -H "Authorization: Bearer $IRI_TOKEN" -H "Accept: application/json" "$@" "$url")
    CODE=${out##*$'\n'}
    BODY=${out%$'\n'*}
}

# fs OP: GET <base>/api/v1/filesystem/OP/<resource_id>?path=TARGET, then follow its task.
# Returns 0 when the call (and its task) completed.
fs() {
    local op=$1 task_id task_uri status i
    echo ">>> GET $BASE_URL/api/v1/filesystem/$op/$RESOURCE_ID?path=$TARGET"
    http "$BASE_URL/api/v1/filesystem/$op/$RESOURCE_ID" --data-urlencode "path=$TARGET"
    echo "HTTP $CODE"
    printf '%s\n' "$BODY" | pretty
    [[ $CODE == 2?? ]] || return 1

    task_id=$(printf '%s' "$BODY" | json_get task_id)
    task_uri=$(printf '%s' "$BODY" | json_get task_uri)
    [[ -n $task_id && -n $task_uri ]] || return 0   # answered directly, no task
    [[ $task_uri == http* ]] || task_uri=$BASE_URL/${task_uri#/}

    echo ">>> task $task_id: polling $task_uri"
    for ((i = 1; i <= TASK_MAX_POLLS; i++)); do
        http "$task_uri"
        if [[ $CODE != 2?? ]]; then
            echo "task poll $i: HTTP $CODE"
            printf '%s\n' "$BODY" | pretty
            return 1
        fi
        status=$(printf '%s' "$BODY" | json_get status)
        echo "task poll $i: $status"
        case $status in
            completed)
                printf '%s\n' "$BODY" | pretty
                return 0 ;;
            failed|canceled)
                printf '%s\n' "$BODY" | pretty
                return 1 ;;
        esac
        ((i < TASK_MAX_POLLS)) && sleep "$TASK_POLL_INTERVAL"
    done
    echo "task $task_id not finished after $TASK_MAX_POLLS polls" >&2
    return 1
}

if fs stat; then
    exit 0
fi
if [[ $CODE == 403 || $CODE == 501 || -n ${status:-} ]]; then
    echo "stat failed; trying ls" >&2
    fs ls
else
    exit 1
fi
