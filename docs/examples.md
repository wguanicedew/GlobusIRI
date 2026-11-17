# Job examples

All examples below use one shared local WORK_DIR. `init` copies the YAML files into
`$GLOBUSIRI_WORK_DIR/jobs/` and keeps any existing files. Run it again after upgrading to
install newly added examples. Edit `config/sites.yaml` for your site paths, endpoints, and
allocation before submitting. The allocation and storage paths shipped here are examples.

```bash
export GLOBUSIRI_WORK_DIR="$HOME/globusiri-work"
globusiri init
globusiri paths
```

For each site you use, log in once and refresh the access tokens. Shell examples need IRI
and HTTPS tokens, since they download logs/results; the transfer example also needs E2E.

```bash
globusiri iri login NERSC_Perlmutter
globusiri https login NERSC_Perlmutter
globusiri iri refresh NERSC_Perlmutter
globusiri https refresh NERSC_Perlmutter
# For Crux, repeat these commands with ALCF_CRUX.
# For E2E transfers:
globusiri e2e login
globusiri e2e refresh
```

Use the [refresh cron examples](cli.md#cron-replaces-harvester_hpccronsh) to keep access
tokens valid for long-running jobs. These shell examples do not need PanDA credentials.

## Hello world on Perlmutter or Crux

The packaged files are
[`perlmutter_hello_job.yaml`](../src/globusiri/data/jobs/perlmutter_hello_job.yaml) and
[`crux_hello_job.yaml`](../src/globusiri/data/jobs/crux_hello_job.yaml). Each prints a host name
and date, writes `result.txt`, and downloads the logs and result after completion. They use
the packaged `shell_job_template.sh`, whose command comes from `template_vars.command`.

```bash
# Inspect the generated script and resource request without submitting.
globusiri job submit --job perlmutter_hello_job.yaml --dry-run
globusiri validate --job perlmutter_hello_job.yaml

globusiri submit --job perlmutter_hello_job.yaml --run-id hello-pm-001
globusiri submit --job crux_hello_job.yaml --run-id hello-crux-001
globusiri monitor
```

`monitor` makes one pass, then exits. Keep calling it or run `globusiri daemon --interval 60`
until the runs finish. Results are saved as `runs/hello-pm-001/result.txt` and
`runs/hello-crux-001/result.txt` under WORK_DIR. Use a new run ID for each submission, or
omit `--run-id` to generate a unique ID. Job files can be reused without editing.

### Retry a failed run

A run that failed, for example at token validation or because a facility API returned an error,
can be retried under the same run ID:

```bash
globusiri runs hello-pm-001                     # which step failed, and why
globusiri submit --resume hello-pm-001          # reopen it and make one pass
globusiri monitor --resume hello-pm-001 hello-crux-001   # same, for several runs
globusiri monitor                               # continue as usual (or keep the daemon running)
```

`--resume` makes the run `active` again and sets its failed steps back to `pending`. Steps
already done are kept, so a job that was already submitted is not submitted again.
`submit --resume RUN_ID --no-validate` also skips token validation for that run. Running
`submit --job ... --run-id hello-pm-001` again does not work, because a run ID is never reused.
`globusiri run --resume RUN_ID` resumes a run and waits for it to finish. See
[cli.md](cli.md#non-blocking-workflow-submit-monitor-daemon).

A run whose files could not be moved ends `upload_failed` (nothing submitted) or
`download_failed` (the job finished, but some files are missing locally). Name it to `monitor`,
without `--resume`, to redo just that transfer:

```bash
globusiri runs --status download_failed
globusiri monitor 20261117-100957-d231ec   # downloads only the files that failed
```

`monitor` and the daemon also retry these runs every 10 minutes, at most 3 times
(`defaults.transfer_retry_interval`, `transfer_retries`). After that the run is
`final_upload_failed` or `final_download_failed`. Old runs can be removed with `globusiri clean`:

```bash
globusiri clean --older-than 7d --dry-run
globusiri clean --older-than 7d --remote   # also the run directories on the HPC
```

## Your own job script (no template)

[`perlmutter_script_job.yaml`](../src/globusiri/data/jobs/perlmutter_script_job.yaml) sets
`script_file: perlmutter_script_job.sh` instead of `template_file`. The
[script](../src/globusiri/data/jobs/perlmutter_script_job.sh) is uploaded as `executable_batch`
exactly as written; `{...}` is not substituted. IRI starts the script in the run directory with the
job file's `resources`. On Perlmutter, NERSC's IRI starts it with `srun`, once per rank
(`n_node × n_process_per_node` copies), so **on Perlmutter, do not call `srun` in the job script**; see below.

```bash
globusiri job submit --job perlmutter_script_job.yaml --dry-run         # show what is uploaded
globusiri submit --job perlmutter_script_job.yaml
globusiri submit --job perlmutter_hello_job.yaml --set script_file=./my_job.sh   # any job file
```

[`crux_script_job.yaml`](../src/globusiri/data/jobs/crux_script_job.yaml) does the same on Crux
with [`crux_script_job.sh`](../src/globusiri/data/jobs/crux_script_job.sh). There, PBS runs the
script once, so the script starts the ranks itself with `mpiexec`. Crux's IRI sends PBS only the
node count, so the script sets the processes per node itself (`PPN=4`, `DEPTH=32`). It takes the node count from
`$PBS_NODEFILE`, so `--set resources.n_node=2` gives 8 ranks without editing the script.

```bash
globusiri submit --job crux_script_job.yaml --set resources.n_node=2
```

A relative `script_file` is looked up in the current directory, next to the job file, then in
WORK_DIR. Inputs are uploaded next to the script as separate files. With `archive_inputs: true`,
they are uploaded as one archive instead, and the `GLOBUSIRI_ARCHIVE` stage-in block that extracts
it is inserted after the script's header; the scripts above therefore don't contain it.

## Several processes per node (MPI-style launch)

[`perlmutter_mpi_job.yaml`](../src/globusiri/data/jobs/perlmutter_mpi_job.yaml) and
[`crux_mpi_job.yaml`](../src/globusiri/data/jobs/crux_mpi_job.yaml) run 4 processes per node.
Each process prints its rank and host. The sorted lines are downloaded as `ranks.txt`.

```bash
globusiri submit --job perlmutter_mpi_job.yaml --run-id mpi-pm-001
globusiri submit --job crux_mpi_job.yaml --run-id mpi-crux-001 --set resources.n_node=2   # 8 ranks
globusiri monitor
```

The two facilities start the ranks differently, so the two examples are written differently:

| | Perlmutter (Slurm) | Crux (PBS) |
|---|---|---|
| What IRI receives | `node_count`, `process_count`, `processes_per_node`, `cpu_cores_per_process` | `node_count` only (the per-process fields are in `unsupported_resource_fields`) |
| How IRI runs `executable_batch` | under `srun`, **once per rank** | **once**, on the first node |
| Who starts the ranks | NERSC's IRI | the script: Cray PALS `mpiexec -n $GLOBUSIRI_NPROC --ppn $GLOBUSIRI_NPROC_PER_NODE --depth $GLOBUSIRI_NCORE_PER_PROC --cpu-bind depth` |
| `command` is | the per-rank work (`$SLURM_PROCID` is the rank) | the launch of the ranks |

**On Perlmutter, do not call `srun` in the job script.** The ranks already hold the job's CPUs, so a
nested `srun` waits ("step creation temporarily disabled ... Requested nodes are busy") until the
job's time limit. In the example, each rank writes `rank_<N>.txt`, and rank 0 waits for all of them and
gathers them into `ranks.txt`. `n_core_per_node: 256` counts Slurm CPUs (hyperthreads), so each
rank gets 64.

On Crux, the script runs once and starts the ranks with `mpiexec`. Crux's IRI does not take the
per-process fields, so the script's `mpiexec` is the only thing that sets the rank layout.
`n_core_per_node: 128` counts physical cores, so each rank gets 32. The `cray-pals` module is loaded
if `mpiexec` is not on `PATH`.

`shell_job_template.sh` exports the `GLOBUSIRI_*` variables from `resources`:
`GLOBUSIRI_NNODE`, `GLOBUSIRI_NPROC_PER_NODE`, `GLOBUSIRI_NPROC` (nodes × per node) and
`GLOBUSIRI_NCORE_PER_PROC` (`n_core_per_node / n_process_per_node`). `--set
resources.n_node=...` therefore changes the layout without editing the command. For a real MPI
program, add the executable to `inputs`. On Perlmutter, run it as the per-rank command. On Crux,
put it in place of `bash -c '...'` after `mpiexec`.

Setup such as input extraction happens before the ranks do their work at both sites. On
Perlmutter, every rank runs the script, so the stage-in block has rank 0 extract the inputs while
the other ranks wait.

The PanDA templates `perlmutter_iri_submit_template.sh` and `crux_iri_submit_template.sh` are
Harvester pilot templates, not standalone job scripts. The Perlmutter one also runs once per rank;
its output lines are prefixed with `pilot_${SLURM_PROCID}`. Use `shell_job_template.sh` (or your own
template or `script_file`) for your own jobs.

## Upload a local input and retrieve an output

[`perlmutter_wordcount_job.yaml`](../src/globusiri/data/jobs/perlmutter_wordcount_job.yaml)
uploads `inputs/words.txt` as `words.txt`, counts words on the HPC, and downloads
`wordcount.txt`. Relative local input paths are relative to WORK_DIR.

```bash
printf 'one two three\n' > "$GLOBUSIRI_WORK_DIR/inputs/words.txt"
globusiri submit --job perlmutter_wordcount_job.yaml --run-id count-001
globusiri daemon --interval 60
# After count-001 finishes, Ctrl-C stops the daemon.
cat "$GLOBUSIRI_WORK_DIR/runs/count-001/wordcount.txt"   # 3
```

Override the input for another submission with `--input`:

```bash
globusiri submit --job perlmutter_wordcount_job.yaml \
  --input words.txt=/absolute/path/to/another.txt
```

Each input is uploaded next to `executable_batch` as its own file, over Globus HTTPS
(`remote.upload_method`). Downloads also use Globus HTTPS. Missing input files are skipped; the
word-count script explicitly fails if `words.txt` is missing.

For jobs with many input files, pack them into one upload instead:

```bash
globusiri submit --job perlmutter_wordcount_job.yaml --set archive_inputs=true
```

The inputs are then uploaded as a single `globusiri_inputs.tar.gz`, and the job script unpacks it
before `command` runs. `shell_job_template.sh` contains a `GLOBUSIRI_ARCHIVE` stage-in block for
this: rank 0 extracts the archive, and any other ranks wait until it is done. You can see the
block with `globusiri job submit --job perlmutter_wordcount_job.yaml --dry-run`. Without an archive,
`GLOBUSIRI_ARCHIVE` is empty and the block does nothing. Your own templates and `script_file`
scripts do not need the block: GlobusIRI inserts it after their header when there is an archive.
See [Input archive](configuration.md#the-globusiri_archive-stage-in-block).

## E2E stage-in, compute, and stage-out

[`perlmutter_transfer_job.yaml`](../src/globusiri/data/jobs/perlmutter_transfer_job.yaml)
demonstrates the full sequence:

1. Globus Transfer copies an existing storage-side `input.txt` onto Perlmutter.
2. IRI submits a shell job that writes `wordcount.txt`.
3. Globus HTTPS downloads logs and the word count locally.
4. Globus Transfer copies the word count to the storage destination.

Before submitting, edit the copied YAML so that storage paths refer to files and writable
directories you can access. The HPC paths must match `remote.work_dir` from your site config
and the chosen run ID. Both source/destination endpoint labels come from the site's E2E
configuration. HPC paths are mount paths; GlobusIRI applies the endpoint's path map.

```bash
globusiri validate --job perlmutter_transfer_job.yaml
globusiri submit --job perlmutter_transfer_job.yaml --run-id transfer-demo-001
globusiri daemon --interval 60
```

This example contains literal paths with `transfer-demo-001`. For a second copy, change
the run ID and the transfer paths together, including the storage output destination, so
jobs have separate output files. Transfer paths do not expand `{runID}` placeholders.
Automatic cleanup is disabled; inputs and remote directories remain available for inspection.

For larger file lists, replace inline `files` with manifests:

```yaml
staging:
  e2e:
    enabled: true
    stagein:
      manifest: inputs/stagein.txt
    stageout:
      manifest: inputs/stageout.txt
```

Each manifest line is `SOURCE_PATH DESTINATION_PATH`. Manifest paths are local and relative
to WORK_DIR; paths inside the manifests describe files on the Globus endpoints. Keep separate
manifests per job when paths differ.

## Rucio deterministic paths: Perlmutter → SDCC → Perlmutter and Crux

[`tests/live/e2e_rucio_chain.sh`](../tests/live/e2e_rucio_chain.sh) is a live E2E test. It uses
real Globus transfers with your E2E token and is not run by `pytest`:

```bash
# 1+2. Perlmutter file -> SDCC as user.wguan:fileA, at its Rucio deterministic path
globusiri e2e stageout NERSC_Perlmutter \
    /global/cfs/cdirs/m2616/harvester_workdir/pilot_env/perlmutter_iri_main.sh \
    /dcint-door003/usatlas.bnl.gov/atlasdatadisk/rucio --rucio-deterministic --scope user.wguan --name fileA

# 3. fileA from SDCC -> the Perlmutter and Crux rucio dirs (same deterministic path)
globusiri e2e stagein NERSC_Perlmutter --rucio user.wguan:fileA
globusiri e2e stagein ALCF_CRUX --rucio user.wguan:fileA
```

| Copy | Path |
|---|---|
| SDCC | `/dcint-door003/usatlas.bnl.gov/atlasdatadisk/rucio/user/wguan/cb/6a/fileA` |
| Perlmutter | `/global/cfs/cdirs/m2616/harvester_workdir/rucio/user/wguan/cb/6a/fileA` |
| Crux | `/ATLAS_workflow_ALCF/usatlas/IRI_workdir/rucio/user/wguan/cb/6a/fileA` (Globus path of `/lus/eagle/projects/...`) |

In step 2, `--rucio-deterministic --scope user.wguan` turns the destination `.../rucio` into a
directory, and the file goes to its Rucio deterministic path below it. `--name` renames it to `fileA`; without `--name`, it keeps the
source file name. In step 3, `--rucio SCOPE:NAME` builds both sides from the site config:
`e2e.stagein.src_path` at SDCC and `e2e.base_path` on the HPC. The files are only placed by path;
nothing is registered in Rucio. Run `tests/live/e2e_rucio_chain.sh [SCOPE] [NAME]` to do all three
steps, and remove the HPC copies afterwards with `globusiri e2e cleanup SITE --rucio
user.wguan:fileA`.

In a job file, put `rucio_deterministic: true` and `scope:` under `staging.e2e` for every
transfer, or under `stagein`/`stageout`. A `files` entry can override them, and set `name`:
`{src, dst, rucio_deterministic, scope, name}`. It can also use the string form `"SRC DST
SCOPE[:NAME]"`. See the comments in
[`perlmutter_transfer_job.yaml`](../src/globusiri/data/jobs/perlmutter_transfer_job.yaml).

## Many submissions, one monitor

Every terminal, cron job, and service must use the same `GLOBUSIRI_WORK_DIR`. Omit run IDs
to generate a fresh ID each time. Independent submitters may run at the same time:

```bash
for i in 1 2 3 4 5; do
  globusiri submit --job perlmutter_hello_job.yaml --set "name=hello-$i" &
done
wait

globusiri runs --status active
globusiri monitor                 # one pass over every active run at every site
globusiri daemon --interval 60    # continuous passes; new submissions join automatically
```

Standalone Globus transfers join the same monitor. Each `e2e stagein`/`stageout --no-wait`
submits one transfer and returns. `runs` lists the transfers with the jobs, and `monitor`
checks them:

```bash
for f in fileA fileB fileC; do
  globusiri e2e stagein NERSC_Perlmutter --rucio user.wguan:$f --no-wait
  globusiri e2e stagein ALCF_CRUX --rucio user.wguan:$f --no-wait
done
globusiri runs --status active    # six transfer runs, STEP e2e_stagein
globusiri monitor                 # one pass: checks every Globus task
```

The daemon can already be running when jobs are submitted. It continues when no runs are
active. Instead of a daemon, a host program or cron can invoke `monitor` periodically.
Additional monitors are safe: a monitor skips any run another process is advancing. Only one
daemon may run per WORK_DIR. `globusiri monitor RUN_ID` restricts a pass to one run.

Bookkeeping lives in `runs/index.db`, with full progress and job snapshots in each run's
directory. Normal monitoring queries active index rows without scanning all directories.
The index is created from saved states when missing; `globusiri runs --reindex` rebuilds it
after importing runs or recovering an interrupted update. Use local storage with POSIX
locks and SQLite WAL support. See the [concurrency details](cli.md#non-blocking-workflow-submit-monitor-daemon).

To keep the index in MySQL/MariaDB, for example next to Harvester, install the driver and
point `sites.yaml` at the server; nothing else changes:

```bash
pip install 'globusiri[mysql]'
export GLOBUSIRI_DB_PASSWORD=...
```

```yaml
# <WORK_DIR>/config/sites.yaml
database:
  url: mysql+pymysql://harvester:${GLOBUSIRI_DB_PASSWORD}@dbhost:3306/harvester
```

`globusiri paths` shows the database in use (password hidden). The first `monitor` or `runs`
creates the `globusiri_runs` and `globusiri_meta` tables and imports the existing runs of
this WORK_DIR. See [`database`](configuration.md#database).

## PanDA pilot jobs

The original [`perlmutter_example_job.yaml`](../src/globusiri/data/jobs/perlmutter_example_job.yaml)
and [`crux_example_job.yaml`](../src/globusiri/data/jobs/crux_example_job.yaml) use the
facility-specific PanDA pilot templates. Set their credential input paths, queue/template
variables, and site environment before using them. They can share the same monitor as the
shell examples:

```bash
globusiri submit --job perlmutter_example_job.yaml
globusiri submit --job crux_example_job.yaml
globusiri monitor
```
