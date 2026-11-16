# CLI reference

Every command below is also available from Python as a method of `globusiri.GlobusIRI`
(see [api.md](api.md)).

## Install

```bash
pip install "globusiri[panda]"            # drop [panda] if PanDA secret sync is not needed
export GLOBUSIRI_WORK_DIR=/path/to/globusiri_work   # optional, see configuration.md#work_dir
globusiri init                            # create WORK_DIR layout, config/sites.yaml, and jobs/*_example_job.yaml
globusiri paths                           # show resolved WORK_DIR, config, tokens, logs
```

`init [--force] [--sites NERSC_Perlmutter,ALCF_CRUX]` never overwrites an existing
`sites.yaml` or job file unless `--force` is given.

## Usage

```
globusiri [--config PATH] [--debug] <group> <command> [SITE] [options]
```

Every command accepts `--config` and `--debug`. Commands that operate on a remote site take
`SITE`, which is a key under `sites:` such as `NERSC_Perlmutter` or `ALCF_CRUX`. Exit codes are
listed in [design.md §9](design.md#9-errors-and-exit-codes).

## Token commands

The `iri`, `https`, and `e2e` groups share the same token subcommands.

| Command | Where | What it does |
|---|---|---|
| `login SITE [--force] [--prompt-login] [--import FILE]` | token host, interactive | Opens a Globus consent URL, reads the auth code, and writes the refresh token. `--import` takes an existing refresh token file instead (e.g. `~/.globus/nersc_auth_tokens.json`, or the old `*_refresh_token.yaml` / `usatlas_hpc_globus_transfer_refresh.yaml`), for migrating without logging in again |
| `refresh SITE [--min-lifetime 6h] [--upload-to-panda] [--no-validate]` | token host, cron | Exchanges the refresh token for an access token and writes the access token file. Never prompts |
| `upload_to_panda SITE` | token host | Stores the access token file as the PanDA user secret |
| `download_from_panda SITE` | runtime host, cron | Fetches the PanDA user secret and writes the access token file |
| `status SITE` | any | Offline only: shows whether the files exist, their permissions, and the time until expiry. Never prints a token. For live checks, use [`globusiri validate`](#validate-tokens) |

For `e2e`, `SITE` is optional and ignored, because the Transfer token is shared by all sites.
`--all` runs the command for every site.

```bash
# One-time, interactive
globusiri iri   login NERSC_Perlmutter
globusiri iri   login ALCF_CRUX
globusiri https login NERSC_Perlmutter
globusiri https login ALCF_CRUX
globusiri e2e   login

# Inspect
globusiri iri status --all
globusiri validate              # live check of every token for every site
```

Requesting a scope only asks Globus for consent. It does not make you log in with a site's
identity. To be asked to log in to each site, list the identity domains in
`e2e.endpoints.<LABEL>.session_domains` (for `e2e login`), `sites.<SITE>.iri.session_domains`
(for `iri login`) or `sites.<SITE>.https.session_domains` (for `https login`). Login then prints one URL per domain. Open each URL in the same browser,
log in with that site's identity, and press Enter. After the last domain, the usual token
request runs with all those identities in the session. Use `--prompt-login --force` to start
from a fresh Globus session.

## Validate tokens

```
globusiri validate [SITE...] [--job JOB_FILE] [--kinds iri,https,e2e] [--offline]
                   [--write-test] [--panda] [--min-lifetime 6h] [--strict] [--json]
```

This command validates all three token kinds (IRI, HTTPS, and E2E), each against its own
service. It never prints a token.

**What gets checked:**

- **With no options**, it checks every token kind for every site. This is the mode for cron
  and for monitoring the deployment.
- **`--job JOB_FILE`** checks only the tokens that job needs, at the job's site (see
  [configuration.md: Tokens a job requires](configuration.md#tokens-a-job-requires)).
  - IRI is always checked.
  - HTTPS is checked when the job uploads or downloads over HTTPS.
  - E2E is checked only when the job has `staging.e2e.enabled: true`. A job without E2E
    transfers never fails because of the E2E token.
- **`SITE...`** limits the check to those sites.
- **`--kinds`** limits it to some token kinds. Combined with `--job`, it narrows that job's
  required kinds further and cannot add kinds.

The output lists every kind that was skipped and why, for example
`e2e  skipped  job does not enable e2e`.

| Kind | Offline checks (always) | Live checks (skipped with `--offline`) |
|---|---|---|
| `iri` | Access file exists and has mode 0600; not expired; lifetime ≥ `--min-lifetime`; `base_url` and `resource_id` match `sites.yaml`; refresh file present on the token host | NERSC: `GET /api/v1/account/projects`. ALCF: filesystem `ls` on Home. Then IRI `stat` on `remote.work_dir`, which checks the token can reach the site's work area |
| `https` | Same file checks; `client_id`, `collection_id`, and `https_server` present; `collection_id` matches a pinned `https.collection_id` if one is set | `GET` on `remote.work_dir` (mapped through `https.path_map`) at `https_server`. A 401 or 403 means a token or consent problem; a 404 means a path or `path_map` problem. `--write-test` also does a `PUT` and then a `GET` of `<work_dir>/.globusiri_validate` (a small file that is overwritten each time) |
| `e2e` | Same file checks; `client_id` present | `get_endpoint` for each endpoint in `e2e.endpoints` (checks that the token is accepted and the `id` exists). `operation_ls` on each site's `e2e.base_path` (mapped through that endpoint's `path_map`) and on the storage `src_path`/`dst_path`. `ConsentRequired` names the endpoint that is missing `data_access` consent |

- **`--panda`.** Also reads each PanDA secret and compares its `expires_at` with the local
  file. It warns when they differ, which means `upload_to_panda` or `download_from_panda`
  has not run since the last refresh. This needs `globusiri[panda]`.
- **Results.** Each check reports `OK`, `WARN`, or `FAIL`, with a one-line reason and, where
  one applies, a fix. For example: `run: globusiri https login ALCF_CRUX`.
  - `WARN` covers expiry within `--min-lifetime`, loose file permissions, and a stale PanDA
    secret.
  - `FAIL` covers a missing or expired token, a rejected token, missing consent, and an
    unreachable path.
- **Exit code.** `0` if nothing failed (and, with `--strict`, nothing warned). Otherwise `7`.
  A configuration error exits `2`.
- **`--json`.** Prints one object per check: `site`, `kind`, `check`, `status`, `detail`,
  `expires_at`. This is meant for monitoring.

```
$ globusiri validate
SITE              KIND   CHECK            STATUS  DETAIL
NERSC_Perlmutter  iri    token file       OK      expires in 41h
NERSC_Perlmutter  iri    api              OK      GET account/projects -> 200
NERSC_Perlmutter  iri    work_dir         OK      stat /global/cfs/cdirs/m2616/harvester_workdir
NERSC_Perlmutter  https  token file       OK      expires in 45h
NERSC_Perlmutter  https  read work_dir    OK      GET -> 200
ALCF_CRUX         https  read work_dir    FAIL    GET -> 403; run: globusiri https login ALCF_CRUX
-                 e2e    endpoint Crux    OK      ls /ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio
-                 e2e    endpoint BNL     FAIL    ConsentRequired data_access; run: globusiri e2e login --force
```

### Cron (replaces `harvester_HPC/cron.sh`)

Cron does not load your shell environment. Set `GLOBUSIRI_WORK_DIR` in the crontab, and
call `globusiri` by its full path inside the venv.

Single-host mode, where the token host is also the runtime host:

```cron
GLOBUSIRI_WORK_DIR=/path/to/globusiri_work
PATH=/path/to/venv/bin:/usr/bin:/bin
# m h dom mon dow  command
0 */2 * * *  globusiri iri   refresh --all --min-lifetime 6h --upload-to-panda
5 */2 * * *  globusiri https refresh --all --min-lifetime 6h --upload-to-panda
10 */2 * * * globusiri e2e   refresh       --min-lifetime 6h --upload-to-panda
30 */2 * * * globusiri validate --json >> $GLOBUSIRI_WORK_DIR/logs/validate.jsonl || <alert command>
```

Split mode adds this on the runtime (Harvester) host, which uses its own WORK_DIR:

```cron
GLOBUSIRI_WORK_DIR=/path/to/runtime_work
20 * * * *   globusiri iri   download_from_panda --all
21 * * * *   globusiri https download_from_panda --all
22 * * * *   globusiri e2e   download_from_panda
```

## IRI job commands

```bash
globusiri job submit [SITE] --job JOB_FILE [--run-id ID]
                     [--set key=value]... [--input NAME=LOCAL]... [--dry-run] [--no-validate]
globusiri job status SITE JOB_ID [--wait] [--interval 60] [--json]
globusiri job cancel  SITE JOB_ID
globusiri job list    SITE [--historical] [--limit N]
globusiri job cleanup SITE RUN_ID
```

The job file supplies the template, resources, and inputs (see
[configuration.md: Job files](configuration.md#job-files)). `SITE` defaults to the job file's
`site`. `--set` overrides one key for this submission only, for example
`--set resources.n_node=4 --set resources.duration=2h`. The job file itself is never modified.

`job submit` does the following:

1. Merges the site's `job_defaults`, the job file, and any `--set` values, then saves the result to `runs/<run_id>/job.yaml`.
2. Renders the template with Harvester placeholder names.
3. Runs IRI `mkdir -p` on `<work_dir>/<run>` and `<log_dir>/<run>`.
4. Packs the job's `inputs` into `globusiri_inputs.tar.gz` and uploads exactly two files, `executable_batch` and that archive, using `remote.upload_method`. Rank 0 of the job extracts the archive; the other ranks wait for it (see [configuration](configuration.md#input-archive)).
5. Runs `chmod 0755` on `executable_batch`.
6. Submits the job and prints the job ID.

`--dry-run` prints the rendered script and `job_spec` without contacting IRI, and leaves no run
directory behind. Without `--dry-run`, `job submit` first validates the IRI (and, if needed,
HTTPS) token, unless `--no-validate` is given. State is kept in `<WORK_DIR>/runs/<run_id>/`.

`job status --wait` polls until the job reaches a terminal state. It exits with code 0 for
`finished` and 6 for `failed` or `cancelled`.

`job cleanup` removes a run's remote directory, `<remote.work_dir>/<RUN_ID>`, with IRI `rm`. If
`remote.log_dir` is different from `remote.work_dir`, it also removes `<remote.log_dir>/<RUN_ID>`.
A directory that is already gone is reported as `already gone`, not as an error. Download
stdout and stderr first (`https stageout --job-id`), because they are removed with the run
directory.

## IRI filesystem helpers

```bash
globusiri fs ls    SITE PATH [--resource-id ID] [--all]
globusiri fs stat  SITE PATH
globusiri fs mkdir SITE PATH [--parents]
globusiri fs chmod SITE PATH MODE
globusiri fs rm    SITE PATH [--force]
```

`fs rm` removes a file or directory with IRI `rm`. To protect against typos, `PATH` must be
below the site's `remote.work_dir`, `remote.log_dir` or `e2e.base_path`. `--force` removes the
restriction, but `/` is always refused.

## Globus HTTPS staging

`stagein` uploads to the HPC and `stageout` downloads from it. Remote paths are filesystem
paths and are translated through `path_map`.

```bash
globusiri https stagein  SITE LOCAL_FILE... --to REMOTE_DIR [--mkdir] [--executable NAME]
globusiri https stageout SITE REMOTE_FILE... --to LOCAL_DIR [--overwrite]
globusiri https stageout SITE --job-id JOB_ID [--run-id RUN_ID] --to LOCAL_DIR   # stdout/stderr of a job
```

Files that already exist locally are skipped unless `--overwrite` is given.
`stageout --job-id` finds stdout and stderr in the job's `admincomment` (`stdoutPath` and
`stderrPath`). If they are not there, it uses `<log_dir>/<RUN_ID>/<RUN_ID>_std{out,err}.txt`,
which needs `--run-id`.

## Globus E2E staging

```bash
globusiri e2e stagein  SITE (--manifest FILE | --rucio SCOPE:NAME... | SRC DST)
                            [--src-endpoint LABEL] [--sync-level exists] [--no-wait]
globusiri e2e stageout SITE (--manifest FILE | --rucio SCOPE:NAME... | SRC DST)
                            [--dst-endpoint LABEL] [--no-wait]
globusiri e2e cleanup  SITE (--manifest FILE | --rucio SCOPE:NAME... | PATH...)
                            [--force] [--dry-run] [--no-wait]
globusiri e2e task     TASK_ID [--wait] [--failures]
globusiri e2e cancel   TASK_ID
```

A manifest has one `SRC_PATH DST_PATH` pair per line. Lines starting with `#` are ignored.
`--rucio SCOPE:NAME` is used only to generate the Rucio deterministic RSE path, under
`stagein.src_path` (or `stageout.dst_path`) on the storage side and `e2e.base_path` on the HPC
side. It does not contact Rucio: it does not look up replicas, check that the file exists on the
RSE, expand datasets, or register files after stage-out.

`e2e cleanup` removes files that a stage-in wrote to the HPC. It submits a Globus delete task
on the site's HPC endpoint, using the E2E token. It takes the same inputs as `stagein`:

- `--manifest FILE`: the stage-in manifest. Its `DST` column is deleted.
- `--rucio SCOPE:NAME`: the Rucio deterministic path under `e2e.base_path` is deleted.
- `PATH...`: HPC file paths (mount paths).

Paths are translated through the HPC endpoint's `path_map`. They must be below the site's
`remote.work_dir`, `remote.log_dir` or `e2e.base_path`, unless `--force` is given. Only files are
deleted (the delete is not recursive), and files that are already missing are ignored.
`--dry-run` prints the Globus paths without deleting anything.

Rucio paths are deterministic, so another job may be reading the same file under
`e2e.base_path`. Clean them up only when no other job on the site needs them.

## End-to-end run

```bash
globusiri run [SITE] --job JOB_FILE
              [--stagein-manifest in.txt] [--stageout-manifest out.txt] [--no-validate]
              [--set key=value]... [--run-id ID] [--no-wait] [--interval 60]
globusiri run --resume RUN_ID [--no-wait] [--no-validate]
```

`run` is the blocking mode of the workflow: it starts a run and does not return until the
run ends. To start a run and let something else drive it, use [`submit`](#non-blocking-workflow-submit-monitor-daemon).

`run` performs these steps in order: E2E stage-in, then HTTPS upload, then IRI submit, then
IRI monitor, then HTTPS download of stdout/stderr, then E2E stage-out, then cleanup.
Cleanup is off by default. The job file's `cleanup` section turns it on (see
[configuration.md: Job files](configuration.md#job-files)). It runs only when the job finished
successfully and the downloads and stage-out did not fail. Otherwise every file is kept for
debugging, and you can remove them later with `job cleanup` and `e2e cleanup`. Progress is saved in
`<WORK_DIR>/runs/<RUN_ID>/state.json`. If `--run-id` is not given, a run ID is generated.
With `--resume`, steps already marked `done` are skipped, and failed steps are retried
(a failed E2E transfer is resubmitted). `--no-wait` makes one pass and returns, like `submit`.

Before any step runs, `run` validates the tokens the job requires, exactly as
`validate --job` does. If any check fails, the run stops before anything is transferred or
submitted. `--no-validate` skips this check.

`--stagein-manifest` and `--stageout-manifest` override the job file's E2E manifests for this
run only. They also turn on `staging.e2e.enabled` for the run, so the E2E token is then
validated as well.

### Example

```bash
globusiri run --job perlmutter_example_job.yaml \
  --set resources.n_node=2 \
  --stagein-manifest inputs.txt \
  --stageout-manifest outputs.txt \
  --run-id test1                 # state in <WORK_DIR>/runs/test1/
```

## Non-blocking workflow: submit, monitor, daemon

```bash
globusiri submit [SITE] --job JOB_FILE [--set key=value]... [--input NAME=LOCAL]...
                 [--stagein-manifest in.txt] [--stageout-manifest out.txt]
                 [--run-id ID] [--no-validate] [--max-attempts 5] [--json]
globusiri monitor [RUN_ID...] [--json]
globusiri daemon [--interval 60] [--once]
globusiri runs [RUN_ID] [--status active|finished|failed|cancelled] [--reindex] [--json]
globusiri cancel RUN_ID
```

These run the same steps as `run`, without ever waiting on a transfer or a job:

- **`submit`** creates the run (`<WORK_DIR>/runs/<RUN_ID>/state.json` and `job.yaml`) and makes
  the first pass. That pass validates the tokens and starts the E2E stage-in. If the job has
  no stage-in, the pass also uploads the scripts over HTTPS and submits the IRI job. It returns
  right away and prints the run ID.
- **`monitor`** makes one pass over every active run, or only over the given run IDs. For each
  run it checks the current step: the Globus task status, or the IRI job state. When that step
  is done, the pass starts the next one, and it keeps going until a step is still in progress.
  Run it each time you want the runs to move on, for example from a host program's own cycle
  (plugin mode, see [api.md](api.md#example-plugin-mode-no-waiting-no-cron)).
- **`daemon`** runs `monitor` every `--interval` seconds until SIGTERM or Ctrl-C. Only one
  daemon can run per WORK_DIR. `--once` makes one pass and exits.
- **`runs`** lists runs with their status, current step, IRI job ID and last error.
  The table uses the central index only. `--json` loads full state for the selected runs.
  `runs RUN_ID` shows every step of one run; `runs --reindex` rebuilds the index.
- **`cancel`** cancels the IRI job and any Globus transfer that is still running, and marks the
  run `cancelled`.

| Step | Pass starts it | Pass checks | Done when |
|---|---|---|---|
| validate | always (unless `--no-validate`) | — | the tokens the job needs pass |
| e2e_stagein | `staging.e2e.enabled` and stage-in files | Globus task status | every task `SUCCEEDED` |
| upload | after stage-in | — | `executable_batch` and the input archive uploaded, `executable_batch` is 0755 |
| submit | after upload | — | IRI returned a job ID |
| monitor | after submit | IRI job state | `finished`, `failed` or `cancelled` |
| https_stageout | job ended | — | stdout/stderr (and `files`) downloaded |
| e2e_stageout | job `finished` and stage-out files | Globus task status | every task `SUCCEEDED` |
| cleanup | job `finished`, and the job file enables it | Globus delete task | stage-in files deleted, run dirs removed |

**Errors.** A failed Globus transfer, a failed token validation or a config error fails the run
at once (`status: failed`, with `message`). Any other error, such as a network error, an HTTP 5xx,
or a token file briefly missing during a refresh, is recorded as `last_error`. The same step is
retried on the next pass. After `--max-attempts` failed passes in a row, the run fails. A failed log
download is recorded on the step and does not fail the run. To retry a failed run, use
`globusiri run --resume RUN_ID [--no-wait]`.

**Many jobs, one monitor.** All commands must use the same `GLOBUSIRI_WORK_DIR` and site
configuration. Any number of `submit` commands can create runs there. `monitor` with no run
IDs queries every active run, across sites, and makes one pass before exiting. Runs submitted
after that query are picked up on the next pass. Use `daemon` for continuous monitoring, or
invoke `monitor` from a host program or cron. `job submit` is a lower-level command whose
runs are not advanced by this monitor; use `submit` for the full workflow.

**Bookkeeping.** `<WORK_DIR>/runs/index.db` is a central SQLite summary index. Each run also
has `state.json` (full progress), `job.yaml` (resolved job), and `.lock` under
`runs/<run_id>/`. Normal monitoring queries `status = active` in the index and reads only
those runs, without scanning WORK_DIR or completed runs. When the index is missing, it is
initialized by a one-time scan of `runs/*/state.json`. Use `runs --reindex` after importing
run directories or to repair an out-of-date summary after an interrupted save.

**Concurrency.** A per-run `flock` prevents two processes from advancing the same run at
once; another monitor reports it as `busy` and continues with other runs. Creation also
locks the run so competing `submit --run-id` calls cannot overwrite it. A brief
`runs/.index.lock` serializes state publication, index initialization, and reindexing;
SQLite transactions commit index changes together, and atomic file replacement keeps
`state.json` readable. Remote calls happen outside the index lock. Multiple monitors and
submitters can therefore share this bookkeeping; only a second `daemon` is refused.

Keep WORK_DIR on a local filesystem with reliable POSIX locks and SQLite WAL support;
network filesystems such as NFS are unsuitable for this WAL-based index. A process killed
between replacing JSON and committing SQLite may leave the index stale: the JSON remains
the recovery source for `runs --reindex`. Locks prevent concurrent duplicate launches, but
an IRI submission accepted remotely with a lost response can still be submitted again by
a retry (see [design.md](design.md#51a-execution-modes)).

```bash
globusiri submit --job perlmutter_example_job.yaml --run-id test2
globusiri runs                    # STATUS active, STEP e2e_stagein / monitor ...
globusiri monitor                 # check and trigger, as often as you like
nohup globusiri daemon --interval 60 >> "$GLOBUSIRI_WORK_DIR/logs/daemon.out" 2>&1 &
```

See [examples.md](examples.md) for complete job files and several submissions sharing one
monitor.
