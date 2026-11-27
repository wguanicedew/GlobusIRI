#!/bin/bash
# Stat a path on NERSC through the IRI filesystem API with plain curl (no globusiri needed).
#
#   tests/live/iri_stat.sh [PATH]
#
# Token: $IRI_TOKEN, else access_token from <WORK_DIR>/tokens/iri/nersc.access.yaml
#        (refresh it first with: globusiri iri refresh NERSC_Perlmutter).
# Override with $IRI_BASE_URL / $IRI_RESOURCE_ID (defaults: NERSC Perlmutter, as in sites.yaml).
# $IRI_API_VERSION: v1 (default; GET ?path=) or v2 (ALCF; POST with a JSON body {"path": ...}).
#
# The filesystem calls are asynchronous: the reply is {"task_id": ..., "task_uri": ...}, and the
# result comes from polling GET task_uri until its status is completed, failed or canceled
# (every $TASK_POLL_INTERVAL s, at most $TASK_MAX_POLLS times; same as the globusiri client).
# NERSC sometimes answers "Unable to get sfapi data: 403" (in the reply or in the failed task)
# for a moment: such a call is repeated after $SFAPI_RETRY_SLEEP s, and fails only after
# $SFAPI_RETRIES attempts in a row get it.
# If stat fails (HTTP 403/501, or its task fails), the same path is tried with ls, as
# `globusiri validate` does.
set -euo pipefail

TARGET=${1:-/global/cfs/cdirs/m2616/www/PanDA_logs/IRI}
BASE_URL=${IRI_BASE_URL:-https://api.iri.nersc.gov}
RESOURCE_ID=${IRI_RESOURCE_ID:-59e80c79-4dfd-4c53-9c07-7405685fcd37}
API_VERSION=${IRI_API_VERSION:-v1}
[[ $API_VERSION == v1 || $API_VERSION == v2 ]] || { echo "IRI_API_VERSION must be v1 or v2" >&2; exit 1; }
TASK_POLL_INTERVAL=${TASK_POLL_INTERVAL:-5}
TASK_MAX_POLLS=${TASK_MAX_POLLS:-60}
SFAPI_RETRIES=${SFAPI_RETRIES:-3}
SFAPI_RETRY_SLEEP=${SFAPI_RETRY_SLEEP:-10}
SFAPI_ERROR="Unable to get sfapi data"

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

# http URL [curl args...]: GET (-G turns --data-urlencode into query parameters); sets CODE and BODY
http() {
    local url=$1 out
    shift
    out=$(curl -sS -G -w '\n%{http_code}' -H "Authorization: Bearer $IRI_TOKEN" -H "Accept: application/json" "$@" "$url")
    CODE=${out##*$'\n'}
    BODY=${out%$'\n'*}
}

# http_post_json URL JSON: POST JSON; sets CODE and BODY
http_post_json() {
    local out
    out=$(curl -sS -X POST -w '\n%{http_code}' -H "Authorization: Bearer $IRI_TOKEN" -H "Accept: application/json" \
        -H "Content-Type: application/json" --data "$2" "$1")
    CODE=${out##*$'\n'}
    BODY=${out%$'\n'*}
}

# fs_once OP: v1 GET <base>/api/v1/filesystem/OP/<resource_id>?path=TARGET, v2 POST
# <base>/api/v2/filesystem/OP/<resource_id> {"path": TARGET}; then follow its task.
# Returns 0 when the call (and its task) completed. Sets SFAPI_FAILED when it failed with
# the transient NERSC sfapi error.
fs_once() {
    local op=$1 task_id task_uri status i
    TASK_FAILED= SFAPI_FAILED=
    local url=$BASE_URL/api/$API_VERSION/filesystem/$op/$RESOURCE_ID
    if [[ $API_VERSION == v2 ]]; then
        local body
        body=$(python3 -c 'import json, sys; print(json.dumps({"path": sys.argv[1]}))' "$TARGET")
        echo ">>> POST $url $body"
        http_post_json "$url" "$body"
    else
        echo ">>> GET $url?path=$TARGET"
        http "$url" --data-urlencode "path=$TARGET"
    fi
    echo "HTTP $CODE"
    printf '%s\n' "$BODY" | pretty
    if [[ $CODE != 2?? ]]; then
        [[ $BODY == *"$SFAPI_ERROR"* ]] && SFAPI_FAILED=1
        return 1
    fi

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
            [[ $BODY == *"$SFAPI_ERROR"* ]] && SFAPI_FAILED=1
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
                TASK_FAILED=1
                [[ $BODY == *"$SFAPI_ERROR"* ]] && SFAPI_FAILED=1
                return 1 ;;
        esac
        ((i < TASK_MAX_POLLS)) && sleep "$TASK_POLL_INTERVAL"
    done
    echo "task $task_id not finished after $TASK_MAX_POLLS polls" >&2
    return 1
}

# fs OP: fs_once, repeated while it fails with the sfapi error (at most SFAPI_RETRIES attempts)
fs() {
    local op=$1 attempt
    for ((attempt = 1; attempt <= SFAPI_RETRIES; attempt++)); do
        fs_once "$op" && return 0
        [[ -n $SFAPI_FAILED ]] || return 1
        if ((attempt < SFAPI_RETRIES)); then
            echo "$op: '$SFAPI_ERROR' (attempt $attempt/$SFAPI_RETRIES); retrying in ${SFAPI_RETRY_SLEEP}s" >&2
            sleep "$SFAPI_RETRY_SLEEP"
        fi
    done
    echo "$op: '$SFAPI_ERROR' in all $SFAPI_RETRIES attempts; giving up" >&2
    return 1
}

if fs stat; then
    exit 0
fi
if [[ $CODE == 403 || $CODE == 501 || -n $TASK_FAILED ]]; then
    echo "stat failed; trying ls" >&2
    fs ls
else
    exit 1
fi
