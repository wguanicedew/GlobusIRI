# Python API

Every `globusiri` command is also a method of `globusiri.GlobusIRI`, so the same operations
can be called from Python, for example from a Harvester plugin, a notebook, or another tool.
The CLI is a thin layer over this API: it parses arguments, calls one method, and prints the
result.

```python
from globusiri import GlobusIRI

g = GlobusIRI()                       # WORK_DIR and sites.yaml are found the same way as the CLI
g.refresh("iri", all_sites=True, min_lifetime="6h")

sub = g.job_submit("perlmutter_example_job.yaml", set=["resources.n_node=2"])
state, report = g.job_wait("NERSC_Perlmutter", sub["job_id"], interval=60)
g.https_stageout_job_logs("NERSC_Perlmutter", sub["job_id"], to="out/", run_id=sub["run_id"])
g.job_cleanup("NERSC_Perlmutter", sub["run_id"])
```

## Conventions

- **Construction.** `GlobusIRI(config=None, *, work_dir=None)`. `config` is a path to
  `sites.yaml` or an already loaded `globusiri.config.Config`. `work_dir` overrides
  `$GLOBUSIRI_WORK_DIR`. The config is loaded on first use, so `paths_info()` works before
  `init()`.
- **Return values.** Methods return plain data (dicts, lists, or the package's dataclasses)
  and never print. Progress is reported through optional callbacks (`on_update`, `echo`).
- **Errors.** Failures raise `globusiri.errors.GlobusIriError` subclasses: `ConfigError`,
  `LoginRequired`, `PandaError`, `RemoteError`, `JobFailed`, `ValidationFailed`. Each has the
  CLI exit code as `exit_code`, and many have a `fix` command to suggest.
- **Waiting.** CLI flags such as `--no-wait` and `--no-validate` become keyword arguments
  with positive names: `wait=True` and `validate=True`.
- **Token commands** act on one or more targets (one per facility for `iri`, one per site for
  `https`, and the single `e2e` token). They return a list of `TargetResult(kind, target,
  result, error)`. By default the first error is raised. With `raise_errors=False`, every
  target is tried and the error is stored in `error`, which is what the CLI does.

## Methods

| CLI | Python |
|---|---|
| `globusiri init [--sites S] [--force]` | `globusiri.api.init(work_dir=None, *, sites=None, force=False)` |
| `globusiri paths` | `g.paths_info()` → `[(what, path, source)]` |
| `globusiri {iri,https,e2e} login [SITE]` | `g.login(kind, site=None, *, force, prompt_login, import_file)` |
| `globusiri {kind} refresh [SITE \| --all]` | `g.refresh(kind, site=None, *, all_sites, min_lifetime, upload_to_panda, validate, raise_errors)` → `[TargetResult(result=RefreshResult)]` |
| `globusiri {kind} upload_to_panda` | `g.upload_to_panda(kind, site=None, *, all_sites, raise_errors)` |
| `globusiri {kind} download_from_panda` | `g.download_from_panda(kind, site=None, *, all_sites, raise_errors)` |
| `globusiri {kind} status` | `g.token_status(kind, site=None, *, all_sites, raise_errors)` → `[TargetResult(result=TokenStatus)]` |
| `globusiri validate [SITE...]` | `g.validate(sites=None, *, kinds, job, offline, write_test, panda, min_lifetime)` → `[Check]`; use `globusiri.validate.failed(checks, strict)` |
| `globusiri job submit` | `g.job_submit(job, site=None, *, run_id, set, inputs, dry_run, validate)` → `{"run_id", "job_id", "remote_dir", "stdout_path", ...}`; `ValidationFailed.checks` lists the failed checks |
| `globusiri job status` | `g.job_status(site, job_id)` → `(state, report)` |
| `globusiri job status --wait` | `g.job_wait(site, job_id, *, interval, timeout, on_update, raise_on_failure)` |
| `globusiri job cancel` | `g.job_cancel(site, job_id)` |
| `globusiri job list` | `g.job_list(site, *, historical, limit)` → `[{"job_id", "name", "iri_state", "state", "report"}]` |
| `globusiri job cleanup` | `g.job_cleanup(site, run_id)` → `{path: "removed" \| "already gone"}` |
| `globusiri fs ls/stat/mkdir/chmod/rm` | `g.fs_ls`, `g.fs_stat`, `g.fs_mkdir`, `g.fs_chmod`, `g.fs_rm(site, path, *, resource_id, force)` |
| `globusiri https stagein` | `g.https_stagein(site, files, to, *, mkdir, executable)` → remote paths |
| `globusiri https stageout` | `g.https_stageout(site, files, to, *, overwrite)` → `{remote: local}` |
| `globusiri https stageout --job-id` | `g.https_stageout_job_logs(site, job_id, to, *, run_id, overwrite)` |
| `globusiri e2e stagein` | `g.e2e_stagein(site, pairs=(), *, manifest, rucio, src_endpoint, sync_level, wait, on_update, label)` → `{"task_ids", "files", "src", "dst"}` |
| `globusiri e2e stageout` | `g.e2e_stageout(site, pairs=(), *, manifest, rucio, dst_endpoint, sync_level, wait, on_update, label)` |
| `globusiri e2e cleanup` | `g.e2e_cleanup(site, paths=(), *, manifest, rucio, force, dry_run, wait, on_update, label)` → `{"endpoint", "items", "task_ids"}` |
| `globusiri e2e task` | `g.e2e_task(task_id, *, failures)`; `g.e2e_wait(task_ids, *, on_update)` |
| `globusiri e2e cancel` | `g.e2e_cancel(task_id)` |
| `globusiri run --job` | `g.run(job, site=None, *, set, run_id, stagein_manifest, stageout_manifest, validate, wait, interval, echo)` → `RunState` |
| `globusiri run --resume` | `g.resume(run_id, *, validate, wait, interval, echo)` → `RunState` |
| — | `g.run_state(run_id)` → the saved `state.json` as a dict |
| — | `g.resolve_job(job, site=None, *, set, inputs)` → `(Job, Site)`; `g.e2e_plan(site, direction, ...)` → transfer plan without submitting |

`--rucio` / `rucio=["SCOPE:NAME"]` only builds the Rucio deterministic RSE path; it does not
contact Rucio (see [cli.md](cli.md#globus-e2e-staging)).

## Example: submit, wait and clean up with progress

```python
from globusiri import GlobusIRI
from globusiri.errors import GlobusIriError, ValidationFailed

g = GlobusIRI()
try:
    state = g.run("crux_example_job.yaml", set=["resources.queue=debug"], echo=print)
    print(state.data["steps"]["submit"]["job_id"])
except ValidationFailed as exc:
    print("fix tokens first:", exc)
except GlobusIriError as exc:
    print(f"failed (exit {exc.exit_code}): {exc}")
```
