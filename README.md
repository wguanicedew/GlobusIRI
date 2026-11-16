# GlobusIRI

GlobusIRI submits and monitors jobs on DOE HPC facilities through the **IRI API** and moves
files with **Globus**:

- **HTTPS** carries run scripts up to the HPC and brings logs back.
- **E2E Transfer** moves bulk input and output data.

Supported sites: NERSC Perlmutter and ALCF Crux.

```
iri login (once) → iri/https/e2e refresh (cron)
→ [e2e stagein] → https upload → iri submit → iri monitor → https download logs → [e2e stageout]
```

## Install

```bash
pip install "globusiri[panda]"
export GLOBUSIRI_WORK_DIR=/path/to/globusiri_work   # optional; default: per-user data dir
globusiri init && globusiri paths
```

Config, tokens, logs, and run state all live under `GLOBUSIRI_WORK_DIR`. If the variable is
not set, they go in the per-user globusiri data directory.

Three ways to drive a workflow (same steps, one engine that never blocks):

```bash
globusiri run    --job perlmutter_example_job.yaml   # blocking: returns when the run ends
globusiri submit --job perlmutter_example_job.yaml   # start a run and return
globusiri monitor                                    # plugin mode: each call checks every active run and triggers its next step
globusiri daemon --interval 60                       # daemon mode: monitor in a loop
```

The job YAML describes the batch job (`resources`, `template_file`), the inputs uploaded over
HTTPS (`inputs`), and the files moved by E2E (`staging.e2e.stagein/stageout`, as a manifest,
Rucio DIDs, or an inline `files:` list).

> **On Perlmutter, do not call `srun` in the job script.** NERSC's IRI already starts the script
> under `srun`, once per rank (`n_node × n_process_per_node` copies); a nested `srun` waits for
> CPUs the ranks hold until the job's time limit. On Crux the script runs once and starts its
> ranks with `mpiexec`. See [several processes per node](docs/examples.md#several-processes-per-node-mpi-style-launch).

Submit as many jobs as needed, including jobs at different sites. One `monitor` call checks
all active runs in the same WORK_DIR; `daemon` keeps doing that and picks up new submissions:

```bash
globusiri submit --job perlmutter_hello_job.yaml
globusiri submit --job perlmutter_wordcount_job.yaml   # first create WORK_DIR/inputs/words.txt
globusiri submit --job crux_hello_job.yaml
globusiri monitor                 # one pass over all active runs, then exits
globusiri daemon --interval 60    # continuous monitoring
```

Bookkeeping uses `<WORK_DIR>/runs/index.db` (SQLite), with full state in
`runs/<run_id>/state.json`. Normal monitoring queries active runs in the index; it does not
scan the working directory. Concurrent submitters and monitors share file locks and SQLite
transactions. Use a local filesystem with POSIX locking and SQLite WAL support. The index
is rebuilt from saved states when missing, or with `globusiri runs --reindex`.

See [job examples](docs/examples.md) for setup, file uploads, E2E transfers, and running many
jobs with one monitor.

## Python API

Every command is also a method of `globusiri.GlobusIRI`:

```python
from globusiri import GlobusIRI

g = GlobusIRI()
sub = g.job_submit("perlmutter_example_job.yaml", set=["resources.n_node=2"])
state, report = g.job_wait("NERSC_Perlmutter", sub["job_id"])

r = g.submit("perlmutter_example_job.yaml")          # non-blocking workflow
g.monitor()[r["run_id"]]["plugin_status"]            # call each cycle: submitted/running/finished/failed
```

## Documentation

- [docs/design.md](docs/design.md): packaging and local layout, architecture, token model, workflow, and open questions
- [docs/configuration.md](docs/configuration.md): WORK_DIR and the `sites.yaml` schema
- [docs/cli.md](docs/cli.md): `globusiri` commands and cron setup
- [docs/api.md](docs/api.md): the Python API (`globusiri.GlobusIRI`), one method per command
- [docs/examples.md](docs/examples.md): shell jobs, local inputs, E2E transfers, and many-job monitoring
- [src/globusiri/data/sites.example.yaml](src/globusiri/data/sites.example.yaml): example site config for `NERSC_Perlmutter` and `ALCF_CRUX` (fixed per site)
- [src/globusiri/data/jobs/](src/globusiri/data/jobs/): example job files, copied into WORK_DIR by `globusiri init`

## Development

```bash
python -m venv .venv && .venv/bin/pip install -e ".[test]"
.venv/bin/pytest
```

Status: implemented (v0.1.0). The unit tests run without network access. Logins, transfers, and job submission still need to be tried against the real NERSC and ALCF services.
