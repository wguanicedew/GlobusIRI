# Configuration reference

GlobusIRI is configured with one YAML file. The full example ships with the package as
[`globusiri/data/sites.example.yaml`](../src/globusiri/data/sites.example.yaml), and
`globusiri init` copies it into place.

## WORK_DIR

All local files live under WORK_DIR:

1. `$GLOBUSIRI_WORK_DIR`, if it is set.
2. Otherwise, the per-user globusiri data directory, `platformdirs.user_data_dir("globusiri")`.
   That is `~/.local/share/globusiri` on Linux and `~/Library/Application Support/globusiri`
   on macOS.

The fallback is not the installed package directory, because `pip install --upgrade` replaces
that directory. See [design.md §2.2](design.md#22-work_dir).

| What | Location |
|---|---|
| Job files | `--job PATH`, else `<WORK_DIR>/jobs/<name>` |
| Config | `--config`, else `$GLOBUSIRI_CONFIG`, else `<WORK_DIR>/config/sites.yaml` |
| IRI tokens | `<WORK_DIR>/tokens/iri/<facility>.refresh.json`, `<facility>.access.yaml` |
| HTTPS tokens | `<WORK_DIR>/tokens/https/<SITE>.refresh.yaml`, `<SITE>.access.yaml` |
| E2E tokens | `<WORK_DIR>/tokens/e2e/refresh.yaml`, `access.yaml` |
| Logs | `<WORK_DIR>/logs/globusiri.log` (all runs) |
| Run logs | `<WORK_DIR>/runs/<run_id>/globusiri.log`, or the job file's `log` (one run only) |
| Runs | `<WORK_DIR>/runs/<run_id>/` (state, rendered script, stdout/stderr) |
| Run index | `<WORK_DIR>/runs/index.db` (SQLite summaries for discovery and listings), or MySQL/MariaDB with [`database`](#database) |
| Templates | `<WORK_DIR>/templates/<name>`, else packaged `globusiri/data/templates/<name>` |

To see the resolved locations, run `globusiri paths`.

All submitters and monitors for the same set of jobs must share WORK_DIR. Use a local
filesystem with POSIX file locks (and SQLite WAL support for the default index). Active runs are found through
the index, without a directory scan; missing indexes are imported from saved run state.
`globusiri runs --reindex` rebuilds the index on demand. See
[CLI bookkeeping and concurrency](cli.md#non-blocking-workflow-submit-monitor-daemon).

Format rules:

- The config file never contains token values, and token locations do not need to be listed
  in it.
- Relative local paths are resolved against WORK_DIR.
- `${VAR}` is expanded.
- Durations accept seconds (`1800`) or suffixed values (`30m`, `6h`, `2d`).

### Overriding a token file (optional)

Use this only when another program must read a token from a fixed path. For example, a
Harvester plugin that has not yet been migrated to globusiri:

```yaml
token_files:
  e2e.access: /some/path/transfer_access.yaml
  https.NERSC_Perlmutter.access: /some/path/NERSC_https_access_token.yaml
  iri.nersc.access: /some/path/nersc_iri_config.yaml
```

## Top level

| Key | Description |
|---|---|
| `defaults` | Values inherited by every site (see below) |
| `e2e` | The Globus Transfer token and endpoint catalog, shared by all sites |
| `database` | Optional. Where the run index lives: SQLite (default) or MySQL/MariaDB (see below) |
| `sites.<NAME>` | One block per HPC target. `NAME` is the CLI argument, for example `NERSC_Perlmutter`. Fixed per site; per-job settings go in [job files](#job-files) |

### `defaults`

| Key | Default | Description |
|---|---|---|
| `globus_client_id` | `165ae151-86aa-430a-8251-ce565e51998f` | Native app client used for HTTPS and E2E logins |
| `refresh_min_lifetime` | `0` | `refresh` skips the refresh if the token outlives this |
| `retries` | `3` | Quick resends (exponential backoff) on connection errors and HTTP 429/5xx. Slower retries of other errors are per site: `iri.retries`, `https.retries` |
| `transfer_retries` | `3` | How many times `monitor` redoes the upload or download of an `upload_failed` / `download_failed` run before it becomes `final_upload_failed` / `final_download_failed` ([cli.md](cli.md#non-blocking-workflow-submit-monitor-daemon)) |
| `transfer_retry_interval` | `10m` | Wait before `monitor` without run IDs, or the daemon, retries such a run. `monitor RUN_ID` retries at once |

## `database`

The run index summarizes every `submit`/`run`/`e2e` run for `monitor`, `daemon` and `runs`.
It is SQLite at `<WORK_DIR>/runs/index.db` unless `database.url` points to a MySQL or
MariaDB server, such as the Harvester database. Full run states stay in
`<WORK_DIR>/runs/<run_id>/state.json` with either backend.

```yaml
database:
  url: mysql+pymysql://harvester:${GLOBUSIRI_DB_PASSWORD}@dbhost:3306/harvester
  namespace: harvester-prod
```

| Key | Default | Description |
|---|---|---|
| `url` | `sqlite:///<WORK_DIR>/runs/index.db` | SQLAlchemy URL. `mysql+pymysql://USER:PASSWORD@HOST[:PORT]/DB` needs `pip install 'globusiri[mysql]'`; `mysql+mysqldb://` uses mysqlclient. `$GLOBUSIRI_DATABASE_URL` overrides it. Put the password in an environment variable (`${VAR}` is expanded) |
| `namespace` | SQLite: `default`; server: the WORK_DIR `runs` path | Rows that belong to this WORK_DIR. Several WORK_DIRs can share one database; each queries and rebuilds only its own namespace. Hosts sharing one WORK_DIR over a shared filesystem must use the same namespace (set it explicitly if the mount paths differ). At most 191 characters |
| `pool_recycle` | `3600` | MySQL only: seconds before a pooled connection is replaced |
| `pool_size`, `max_overflow`, `pool_timeout` | SQLAlchemy defaults | MySQL only: connection pool settings |

GlobusIRI creates its two tables, `globusiri_runs` and `globusiri_meta`, if they are missing.
It never touches other tables, so it can share the Harvester schema. The database user needs
`CREATE`, `SELECT`, `INSERT`, `UPDATE` and `DELETE` on them. The index is derived data: the
first use of a namespace imports `runs/*/state.json`, and `globusiri runs --reindex`
rebuilds it. `globusiri paths` shows the database in use, with the password hidden.

Writes take the lock `<WORK_DIR>/runs/.index.lock`. With SQLite, only one process (or thread)
writes at a time, and readers take no lock because the file is in WAL mode. With MySQL, the
server handles concurrency, and the lock orders each `state.json` write with its row.

## `e2e`

| Key | Description |
|---|---|
| `panda_secret_key` | PanDA user secret name, for example `USATLAS_HPC_TRANSFER_TOKEN` |
| `sync_level` | `exists` \| `size` \| `mtime` \| `checksum` (default `exists`) |
| `verify_checksum` | Default `true` |
| `max_files_per_task` | Maximum items in one Transfer task (default 1000) |
| `poll_interval`, `timeout` | Settings for waiting on a task |
| `endpoints.<LABEL>.id` | Globus endpoint or collection UUID |
| `endpoints.<LABEL>.query`, `.pick` | `endpoint_search` query and result index (same as `ENDPOINT_QUERIES`). If `id` is also set, login fails when the two disagree |
| `endpoints.<LABEL>.data_access` | `true` means `e2e login` requests `data_access` and `https` consent for it |
| `endpoints.<LABEL>.session_domains` | Identity domains (e.g. `[bnl.gov]`) that `e2e login` makes you log in with, one URL per domain in the same browser, before it requests the token. Requesting scopes only asks for consent, so without this Globus issues the token for whatever identity your browser session already has. Use the domain of the identity the collection requires (Globus web app, Settings, Identities). Default: none |
| `endpoints.<LABEL>.path_map` | Map from HPC mount paths to this endpoint's Globus paths. Omit it when the paths are identical (see [`path_map`](#path_map)) |

## `sites.<NAME>`

| Key | Description |
|---|---|
| `facility` | `nersc` \| `alcf`. Selects the IRI client ID, scope, and validation call |

### Adding and removing sites

The sites in the shipped example (`NERSC_Perlmutter`, `ALCF_CRUX`) are only a starting point.
`<WORK_DIR>/config/sites.yaml` is your own file: add, rename or remove sites to match the
machines, projects and directories you use. `globusiri init` never overwrites it (unless
`--force`), so your edits survive package upgrades.

**Remove a site:** delete its block under `sites:`. You can also start with only some example
sites, for example `globusiri init --sites NERSC_Perlmutter`. Afterwards:

- Job files that name the removed site (`site:` in `<WORK_DIR>/jobs/*.yaml`) fail with
  `unknown site`. Delete them or point them at another site.
- Entries under `e2e.endpoints` that no site uses can stay.
- That site's token files under `<WORK_DIR>/tokens/` are no longer used and can be deleted.

**Add a site:** copy an existing block with the same facility, give it a new name, and change
the values. The name is free text; it is what you pass on the command line
(`globusiri validate MY_SITE`) and in a job file's `site:`.

| Needed | Keys |
|---|---|
| Always | `facility` (`nersc` or `alcf`), `iri.base_url`, `iri.resource_id`, `remote.work_dir` |
| With `upload_method: globus_https` (the default) | an `https` section (`collection_query` or `collection_id`, `path_map`). Without it, set `remote.upload_method: iri` |
| For E2E transfers only | an `e2e` section whose `endpoint` is a label in the top-level `e2e.endpoints` |
| Optional | `remote.log_dir`, `unsupported_resource_fields`, `job_defaults` (project, queue, ...), `iri.sfapi_retries`, ... |

Only the facilities `nersc` and `alcf` are supported. You can have several sites at one
facility, for example a second Perlmutter site for another project or work directory:

```yaml
sites:
  NERSC_Perlmutter_m1234:
    facility: nersc
    iri:
      base_url: https://api.iri.nersc.gov
      resource_id: 59e80c79-4dfd-4c53-9c07-7405685fcd37   # Perlmutter compute
    remote:
      work_dir: /global/cfs/cdirs/m1234/globusiri_workdir
      upload_method: globus_https
    job_defaults:
      project: m1234
    https:
      collection_query: NERSC DTN
      path_map: {}                  # Globus path == Perlmutter mount path
```

Tokens follow the facility and site:

- The **IRI** token is per facility: every `nersc` site shares `tokens/iri/nersc.*`, so a new
  NERSC site needs no new IRI login.
- The **HTTPS** token is per site (`tokens/https/<SITE>.*`), so a new site needs
  `globusiri https login <SITE>`.
- The **E2E** token is shared by all sites.

Then check the new site with `globusiri validate <SITE>`. Copy an example job file, set its
`site:` to the new name, and submit it.

### `iri`

| Key | Description | Harvester queueconfig equivalent |
|---|---|---|
| `base_url` | IRI API root | `--base_url` in `cron.sh` |
| `resource_id` | Compute resource | `iri_resource_id` |
| `panda_secret_key` | Name of the PanDA secret | `--panda_secret_key` |
| `validate` | Make a validation call after `refresh` | `--validate-iri` |
| `session_domains` | Identity domains (e.g. `[nersc.gov]`) that `iri login` makes you log in with, one URL per domain, before it requests the token. Default: none | — |
| `validate_url` | Optional. Overrides the validation URL (default: NERSC `account/projects`; ALCF filesystem `ls` on Home) | `--iri-validate-url` |
| `validate_resource_id`, `validate_path` | Optional. ALCF only: resource and path for the default `ls` (default: Home, `/home/$USER/`) | `--alcf-validate-resource-id`, `--alcf-validate-path` |
| `debug` | Print redacted request traces | `iri_debug` |
| `sfapi_retries` | Attempts per IRI call while NERSC answers `Unable to get sfapi data` (default `3`; at least 1) | — |
| `sfapi_retry_sleep` | Wait between those attempts: seconds, or a duration such as `3s`, `1m` (default `3`) | — |
| `retries` | Attempts in all for any other failed IRI call (an HTTP error, or a failed task), default `3`. Not retried: HTTP 404 and "No such file or directory" (missing path), HTTP 501 (not implemented), 401/403 (token), and job submission, where a lost reply could hide an accepted job | — |
| `retry_sleep` | Wait between those attempts (default `10`) | — |

### `remote`

| Key | Description | Harvester equivalent |
|---|---|---|
| `work_dir` | Parent of the per-run directories (`<work_dir>/<run_id>`) | `remote_work_dir` |
| `work_dir_resource_id` | Filesystem resource for `work_dir`. Defaults to `iri.resource_id` | `remote_work_dir_resource_id` |
| `log_dir` | Parent of the per-run log directories | `remote_log_dir` |
| `log_dir_resource_id` | Defaults to `work_dir_resource_id` | `remote_log_dir_resource_id` |
| `upload_method` | `globus_https` (default) \| `iri`. Controls how `job submit` uploads the job script and input archive. `globus_https` needs the site's `https` section | `upload_method` |

### `path_map`

A path inside a Globus collection can differ from the path where the same directory is
mounted on the HPC. Each Globus collection therefore gets its own map:

- `sites.<S>.https.path_map` for the HTTPS collection
- `e2e.endpoints.<E>.path_map` for a Transfer endpoint

The longest prefix wins, and paths that match no prefix pass through unchanged. These maps
replace `globus_upload_dir`, `globus_download_dir`, the Globus-relative `basePath`, and
`_strip_globus_prefix`.

- **NERSC.** The paths are identical, so the map is `{}`.
- **ALCF.** The map is required:

```yaml
path_map:
  /lus/eagle/projects: /    # /lus/eagle/projects/ATLAS_workflow_ALCF/x  ->  /ATLAS_workflow_ALCF/x
```

All other paths in the config are HPC mount paths. Storage-side paths, such as
`stagein.src_path` on BNL, are already Globus paths and are not mapped.

### `unsupported_resource_fields` and `job_defaults`

The site config describes the site. It is **not edited per job**. Per-job settings go in job
files (see [Job files](#job-files)).

| Key | Description | Harvester equivalent |
|---|---|---|
| `unsupported_resource_fields` | Fields that this IRI backend rejects in `job_spec.resources`. They are always dropped, whatever the job file says. This is a site capability, not a job choice | same |
| `job_defaults` | Optional defaults for any key of a job file, for example `project`, `constraint`, or `custom_attributes` | — |

### `https`

| Key | Description | Harvester equivalent |
|---|---|---|
| `collection_query`, `collection_pick` | `endpoint_search` query and index used by `https login` to discover the HTTPS collection | `SITE_QUERIES` |
| `collection_id` | Optional pin that skips the search. The collection is usually not the same as the site's E2E endpoint (it differs for ALCF). The discovered `collection_id` and `https_server` are stored in the token files, and the runtime reads them from there | — |
| `session_domains` | Like `e2e.endpoints.<LABEL>.session_domains`, for `https login` of this site. Default: none |
| `https_server` | Optional. Pins the HTTPS server address. Together with `collection_id`, it lets `https login` skip the first (search) login | — |
| `path_map` | Map from HPC mount paths to this collection's Globus paths | `globus_upload_dir` / `globus_download_dir` |
| `panda_secret_key` | For example, `NERSC_HTTPS_TOKEN` | `--panda_secret_key` |
| `retries` | Attempts in all for a failed download or upload (default `3`), for example HTTP 404 for a file written at the end of a job that the collection does not see yet, or a transient `GlobusError`. 401/403 (token) are not retried | — |
| `retry_sleep` | Wait between those attempts: seconds, or a duration such as `10s`, `1m` (default `10`) | — |

### `e2e` (per site)

| Key | Description | Harvester equivalent |
|---|---|---|
| `endpoint` | Label in the top-level `e2e.endpoints` for this HPC | `dstEndpoint` (preparator) / `srcEndpoint` (stager) |
| `base_path` | Filesystem path for staged data on the HPC | `basePath` |
| `stagein.src_endpoint`, `stagein.src_path` | Default source | `srcEndpoint`, `Globus_srcPath` |
| `stageout.dst_endpoint`, `stageout.dst_path` | Default destination | `dstEndpoint`, `Globus_dstPath` |

## Job files

A job file describes **one kind of submission**. Users keep as many job files as they like,
for example a debug job, a multi-node job, or a GPU job, and pick one per submission:

```bash
globusiri job submit --job perlmutter_example_job.yaml
globusiri run        --job my_4node_job.yaml --set resources.duration=2h
```

- **Examples.** `globusiri init` copies all packaged job examples into `<WORK_DIR>/jobs/`.
  The `*_example_job.yaml` files show PanDA settings; `*_hello_job.yaml` run simple shell
  commands, `*_mpi_job.yaml` start 4 processes per node, `*_script_job.yaml` run a ready-made script (`script_file`), `perlmutter_wordcount_job.yaml` uploads a local file, and
  `perlmutter_transfer_job.yaml` enables E2E stage-in and stage-out. See
  [examples.md](examples.md) for commands and required edits.
- **Lookup.** `--job NAME` is used as a path if it exists. Otherwise it is looked up as
  `<WORK_DIR>/jobs/NAME`.
- **Merge order.** Later entries win:
  1. `sites.<site>.job_defaults`
  2. the job file
  3. `--set key=value` on the command line (dotted keys)
- **Dict merging.** Dicts such as `custom_attributes` and `inputs` are merged key by key.
  Lists replace the earlier value.
- **Snapshot.** The fully resolved job is saved as `<WORK_DIR>/runs/<run_id>/job.yaml`.
  `--resume` uses that snapshot, so a later edit to the job file does not change a run that has
  already started.

| Key | Description | Harvester equivalent |
|---|---|---|
| `site` | Key in `sites.yaml`. A `SITE` argument on the CLI overrides it | queue name |
| `name` | IRI job name prefix. The run ID is appended | `harvester_id-workerID` |
| `template_file` | Batch script template, rendered with Harvester placeholder names. A bare name is looked up in `<WORK_DIR>/templates/`, then in the packaged templates | `templateFile` |
| `script_file` | A ready job script, used instead of `template_file`. It is uploaded as written, with no `{placeholder}` rendering, and must start with `#!`. A relative path is looked up in the current directory, next to the job file, then in WORK_DIR, and is saved as an absolute path in the run's `job.yaml`. If one layer (site `job_defaults`, job file, `--set`) sets `script_file` and a more general layer sets `template_file`, the more specific one wins. A single layer that sets both is an error | — |
| `template_vars` | Extra or overriding template placeholders | — |
| `resources.project` | Account or allocation | `project` |
| `resources.queue` | Remote queue | `remoteQueueName` |
| `resources.constraint` | Slurm constraint, sent in `custom_attributes` | `constraint` |
| `resources.n_node`, `n_core_per_node`, `n_process_per_node` | Resource request | `nNode`, `nCorePerNode`, `nProcessPerNode` |
| `resources.gpu_cores_per_process` | Omitted when 0 | same |
| `resources.memory` | Bytes. `null` means not requested | derived from `minRamCount` |
| `resources.duration` | Walltime | `duration` |
| `resources.launcher` | `null` \| `single` \| `mpirun` \| `srun` \| `aprun` \| `jsrun` | `launcher` |
| `resources.exclusive_node_use`, `reservation_id` | Pass-through settings | same |
| `resources.custom_attributes` | Merged into `job_spec.attributes.custom_attributes` | same |
| `resources.signal`, `no_requeue`, `licenses`, `export`, `image`, `volume`, `pre_launch`, `post_launch` | Optional pass-through settings | same |
| `inputs.<remote_name>` | Local file uploaded as `<remote.work_dir>/<run_id>/<remote_name>`, or packed into the input archive with `archive_inputs: true` (see [Input archive](#input-archive)). Relative paths are resolved against WORK_DIR, and `${X509_USER_PROXY}` is allowed. Missing files are skipped | `pandaTokenFilename`, `pandaTokenKeyPath`, `x509UserProxy` |
| `archive_inputs` | `false` (default): each input is uploaded as its own file and nothing is extracted. `true`: the inputs are uploaded as one `globusiri_inputs.tar.gz`, which the job script extracts. See [Input archive](#input-archive) | — |
| `panda_auth_origin` | `{tokenOrigin}` placeholder | `pandaAuthOrigin` |
| (derived) | `{inputArchive}`: `globusiri_inputs.tar.gz` if the job has inputs, else empty. See [Input archive](#input-archive) | — |
| `staging.e2e.enabled` | Whether this job moves data with Globus E2E Transfer. If `false` (the default), the E2E steps are skipped and the E2E token is **not required or validated** for this job | preparator / stager configured or not |
| `staging.e2e.stagein.manifest`, `.rucio`, `.files` | Transfer into the HPC before submission (e.g. input ROOT files). `files` is an inline list of `"SRC DST [SCOPE[:NAME]]"` strings or `{src, dst[, rucio_deterministic][, scope][, name]}` maps; HPC-side paths are mount paths. A Rucio-deterministic entry's `DST` is a directory and the file goes to its Rucio deterministic path below it, named `NAME` or the basename of `SRC` (see [cli.md](cli.md#globus-e2e-staging)). The three sources are combined. Used only when `enabled` is true | preparator |
| `staging.e2e.rucio_deterministic`, `staging.e2e.scope` | `rucio_deterministic: false` (default): each `DST` is the destination file. `true`: each `DST` is a directory, and the file goes to its Rucio deterministic path below it, under `scope` (required). The `stagein`/`stageout` sections and each `files` entry can override both | — |
| `staging.e2e.stageout.manifest`, `.rucio`, `.files` | Transfer out of the HPC after the job finished successfully. Same forms as stage-in. Used only when `enabled` is true | stager |
| `staging.https_stageout.stdout_stderr`, `.files` | Files to download after the job ends, into `<WORK_DIR>/runs/<run_id>/` | monitor `download_logs` |
| `staging.https_stageout.local_dir` | Local directory the downloads go to. A relative path is resolved against WORK_DIR, and `{run_id}` is replaced. Default `null` means `<WORK_DIR>/runs/<run_id>/` | worker log directory |
| `cleanup.remote_run_dir` | `true` makes `run` remove `<remote.work_dir>/<run_id>` (and `<remote.log_dir>/<run_id>`) with IRI `rm` at the end. Default `false` | sweeper |
| `log` | Local log file for this run. It gets the same records as `logs/globusiri.log` (INFO and up), but only the ones for this run: IRI upload/submit, job state changes, step changes, HTTPS downloads, E2E tasks and their progress, errors and retries. A relative path is resolved against WORK_DIR, and `{run_id}` is replaced, e.g. `logs/runs/{run_id}.log`. Default `null` means `<WORK_DIR>/runs/<run_id>/globusiri.log` | worker log |
| `cleanup.e2e_stagein` | `true` makes `run` delete the E2E stage-in files on the HPC with a Globus delete task at the end. Used only when `staging.e2e.enabled` is true. Default `false` | — |

### Tokens a job requires

The job file and its site together decide which tokens the job needs. `globusiri validate --job`
and `globusiri run` check only those tokens.

| Token | Required when |
|---|---|
| `iri` | Always (submit, monitor, mkdir, chmod) |
| `https` | The site's `remote.upload_method` is `globus_https`, **or** `staging.https_stageout` downloads anything (`stdout_stderr: true` or a non-empty `files`) |
| `e2e` | `staging.e2e.enabled: true` |

A job file that has E2E manifests or Rucio files while `enabled: false` is rejected as a
configuration error (exit 2), so that data is never silently left untransferred.

`run` cleans up only after the job finished successfully and the HTTPS download and E2E
stage-out did not fail. After a failed download, the run is `download_failed` and its remote
files are kept; `globusiri monitor RUN_ID` downloads the missing files and then cleans up. A failed or cancelled run keeps its files so you can debug it. You can
remove them later with `globusiri job cleanup` and `globusiri e2e cleanup`.


### Input archive

Every submission uploads `executable_batch` to `<remote.work_dir>/<run_id>/`. This is the
rendered `template_file`, or the `script_file` as written, made executable (chmod 0755). How the
`inputs` travel depends on `archive_inputs`:

- `false` (the default): each input is uploaded as its own file, `<remote_name>`. Nothing has to
  be extracted, so the stage-in block below does nothing and is not inserted.
- `true`: every input is packed into one `globusiri_inputs.tar.gz`, stored under its
  `<remote_name>`, so a submission uploads at most two files. The job script extracts the archive
  with the stage-in block below. Use this when a job has many input files, because each upload is
  a separate HTTPS request.

Set it in the job file, or for one submission with `--set archive_inputs=true`.

#### The `GLOBUSIRI_ARCHIVE` stage-in block

The job script extracts the archive with this block, which starts and ends with
`# ---- input stage-in ----` lines. It is in `shell_job_template.sh` and
`crux_iri_submit_template.sh`, right after `cd {work_dir}`:

```sh
GLOBUSIRI_ARCHIVE="{inputArchive}"
if [ -n "$GLOBUSIRI_ARCHIVE" ]; then
    # rank 0 extracts the archive and writes .globusiri_inputs.done;
    # the other ranks wait for that file
    ...
fi
```

- `{inputArchive}` is `globusiri_inputs.tar.gz` when the job has inputs and `archive_inputs` is
  true. Otherwise it is empty, so `GLOBUSIRI_ARCHIVE` is empty and the block does nothing. That
  is why the block can stay in a template whether or not a job has inputs.
- The rank comes from `SLURM_PROCID`, `PMIX_RANK`, `PMI_RANK`, `PALS_RANKID` or
  `OMPI_COMM_WORLD_RANK`. If none is set, it is 0. The block works both when the script runs once
  (Crux, where the script starts `mpiexec` itself) and when it runs once per rank (Perlmutter, where
  NERSC's IRI starts the script under `srun`).
- Rank 0 extracts the archive in the run dir, then creates `.globusiri_inputs.done`. If extraction
  fails, it creates `.globusiri_inputs.failed` and exits.
- The other ranks wait until `.globusiri_inputs.done` appears and then continue. They exit
  immediately if `.globusiri_inputs.failed` appears. They also exit after waiting
  `$GLOBUSIRI_STAGEIN_TIMEOUT` seconds (default 1800).

**Do not copy the block into your own templates or scripts.** If a template never mentions
`{inputArchive}`, or a `script_file` never mentions `globusiri_inputs.tar.gz`, GlobusIRI inserts
the block after its leading `#!`, `#SBATCH`, `#PBS` and comment lines. This happens only when there
is an archive to extract. The Harvester pilot template `perlmutter_iri_submit_template.sh` and the
`*_script_job.sh` examples work this way. Put the block in yourself only to control where it
runs, for example after a `cd` or a module load that has to come first. Copy it unchanged from
`shell_job_template.sh` into a template. In a `script_file`, extract `globusiri_inputs.tar.gz`
yourself instead, because placeholders are not rendered there.

After the block, the script sees the inputs as plain files in the run dir. Placeholders such as
`{pandaTokenFilename}` still resolve to the remote name.

## Token file formats

These are unchanged from the current scripts, so existing Harvester plugins can still read them.

Each token kind (`iri`, `https`, `e2e`) has its own files and its own PanDA secret. The
tokens are not interchangeable.

**IRI access** (`tokens/iri/<facility>.access.yaml`). The PanDA secret holds the same fields as JSON. A secret
that holds only a bare token string can still be read:

```yaml
base_url: https://api.iri.nersc.gov
resource_id: 59e80c79-4dfd-4c53-9c07-7405685fcd37
access_token: <redacted>
expires_at: 1794031209          # new, optional; ignored by IriClient
```

**HTTPS access** (`tokens/https/<SITE>.access.yaml`):

```yaml
client_id: 165ae151-86aa-430a-8251-ce565e51998f
access_token: <redacted>
expires_at: 1794031209
collection_id: 9d6d994a-6d04-11e5-ba46-22000b92c6ec
https_server: https://m-bb53bd.c8d61e.8540.data.globus.org
```

**HTTPS refresh** (`tokens/https/<SITE>.refresh.yaml`) has the same fields, with `refresh_token` in
place of `access_token` and `expires_at`.

**E2E access** (`tokens/e2e/access.yaml`):

```yaml
client_id: 165ae151-86aa-430a-8251-ce565e51998f
access_token: <redacted>
expires_at: <unix time>
NERSC_https: <https_server>
Crux_https: <https_server>
```

All token files are written atomically with mode 0600, inside `tokens/` (mode 0700).
