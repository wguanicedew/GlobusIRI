#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""`globusiri` command line."""

import argparse
import json
import logging
import logging.handlers
import sys
import time
from datetime import datetime, timezone

from . import __version__, jobs
from . import validate as validation
from .api import GlobusIRI
from .api import init as init_work_dir
from .errors import EXIT_OK, EXIT_VALIDATION, ConfigError, GlobusIriError, JobFailed, ValidationFailed
from .paths import Paths
from .tokens import KINDS

log = logging.getLogger("globusiri")


# ---------------------------------------------------------------------- helpers
def setup_logging(paths, debug):
    root = logging.getLogger("globusiri")
    root.setLevel(logging.DEBUG)
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG if debug else logging.WARNING)
    console.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    root.addHandler(console)
    try:
        paths.logs_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(paths.log_file, maxBytes=10 * 2**20, backupCount=5)
        fh.setLevel(logging.DEBUG if debug else logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root.addHandler(fh)
    except OSError:
        pass


def table(rows, headers):
    rows = [[str(c) if c is not None else "" for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)).rstrip()]
    for r in rows:
        lines.append("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip())
    return "\n".join(lines)


def fmt_time(ts):
    if not ts:
        return "-"
    return datetime.fromtimestamp(float(ts), timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_left(ts):
    if not ts:
        return "unknown"
    left = float(ts) - time.time()
    if left <= 0:
        return f"expired {(-left) / 3600:.1f}h ago"
    return f"{left / 3600:.1f}h"


def print_json(obj):
    print(json.dumps(obj, indent=2, default=str))


def print_checks(checks, as_json=False):
    if as_json:
        for c in checks:
            print(json.dumps(c.to_dict()))
        return
    print(table([(c.site, c.kind, c.check, c.status, c.detail_with_fix) for c in checks],
                ["SITE", "KIND", "CHECK", "STATUS", "DETAIL"]))


def print_results(results):
    """Print a {remote: local | 'skipped' | 'error: ...'} map; exit 5 if any failed."""
    rc = EXIT_OK
    for remote, local in results.items():
        print(f"{remote} -> {local}")
        if str(local).startswith("error"):
            rc = 5
    return rc


# ---------------------------------------------------------------------- init / paths
def cmd_init(args, api):
    res = init_work_dir(args.work_dir, sites=args.sites, force=args.force)
    for f in res["kept"]:
        print(f"kept      {f}")
    for f in res["written"]:
        print(f"wrote     {f}")
    print(f"\nWORK_DIR  {res['work_dir']}  (from {res['source']})")
    print("Next: edit config/sites.yaml, put job inputs in inputs/, then run `globusiri iri login SITE`.")
    return EXIT_OK


def cmd_paths(args, api):
    print(table(api.paths_info(), ["WHAT", "PATH", "SOURCE"]))
    return EXIT_OK


# ---------------------------------------------------------------------- tokens
def cmd_token(args, api):
    kind, action = args.group, args.action
    if kind == "e2e" and getattr(args, "site", None):
        print(f"note: the e2e token is shared by all sites; '{args.site}' is ignored", file=sys.stderr)
    if action == "login":
        r = api.login(kind, args.site, force=args.force, prompt_login=args.prompt_login, import_file=args.import_file)
        print(f"{kind} login OK for {r['target']}; refresh token in {r['refresh_file']}; "
              f"access token expires {fmt_time(r['expires_at'])}")
        return EXIT_OK

    target = dict(site=args.site, all_sites=args.all, raise_errors=False)
    if action == "refresh":
        results = api.refresh(kind, min_lifetime=args.min_lifetime, upload_to_panda=args.upload_to_panda,
                              validate=False if args.no_validate else None, **target)
    elif action == "upload_to_panda":
        results = api.upload_to_panda(kind, **target)
    elif action == "download_from_panda":
        results = api.download_from_panda(kind, **target)
    else:
        results = api.token_status(kind, **target)
    rc = EXIT_OK
    for r in results:
        if not r.ok:
            print(f"{kind} {r.target}: ERROR: {r.error}", file=sys.stderr)
            rc = max(rc, r.error.exit_code)
        elif action == "refresh":
            extra = "; uploaded to PanDA" if r.result.uploaded else ""
            print(f"{kind} {r.target}: {r.result.reason}, expires {fmt_time(r.result.expires_at)}{extra}")
        elif action == "upload_to_panda":
            print(f"{kind} {r.target}: uploaded to PanDA secret {r.result}")
        elif action == "download_from_panda":
            print(f"{kind} {r.target}: wrote {r.result['access_file']}, expires {fmt_time(r.result['expires_at'])}")
        else:
            st = r.result
            print(table([
                ("access", st.access_file, ("ok" if st.access_private else "MODE NOT 0600") if st.access_exists else "missing",
                 fmt_left(st.expires_at) if st.access_exists else "-"),
                ("refresh", st.refresh_file, "present" if st.refresh_exists else "absent (runtime host?)", ""),
            ], [f"{kind} {r.target}", "FILE", "STATE", "LIFETIME"]))
    return rc


# ---------------------------------------------------------------------- validate
def cmd_validate(args, api):
    kinds = [k.strip() for k in args.kinds.split(",")] if args.kinds else None
    checks = api.validate(args.sites or None, kinds=kinds, job=args.job, offline=args.offline,
                          write_test=args.write_test, panda=args.panda, min_lifetime=args.min_lifetime)
    print_checks(checks, args.json)
    return EXIT_VALIDATION if validation.failed(checks, args.strict) else EXIT_OK


# ---------------------------------------------------------------------- job
def cmd_job(args, api):
    if args.action == "submit":
        try:
            r = api.job_submit(args.job, args.site, run_id=args.run_id, set=args.set, inputs=args.input,
                               dry_run=args.dry_run, validate=not args.no_validate)
        except ValidationFailed as exc:
            print_checks(getattr(exc, "checks", []))
            return EXIT_VALIDATION
        if args.dry_run:
            print(f"# rendered {jobs.EXECUTABLE} (run_id {r['run_id']})\n")
            print(r["script"])
            print("# job_spec")
            print_json(r["job_spec"])
            for s in r["skipped_inputs"]:
                print(f"# input skipped (not found): {s}")
            return EXIT_OK
        for s in r["skipped_inputs"]:
            print(f"input skipped (not found): {s}", file=sys.stderr)
        print(f"run_id {r['run_id']}\njob_id {r['job_id']}\nremote {r['remote_dir']}\nstdout {r['stdout_path']}")
        return EXIT_OK

    if args.action == "status":
        if args.wait:
            state, report = api.job_wait(args.site, args.job_id, interval=args.interval,
                                         on_update=lambda s, r: (not args.json) and print(f"{args.job_id}: {s}"))
        else:
            state, report = api.job_status(args.site, args.job_id)
        if args.json:
            print_json({"job_id": args.job_id, "state": state, "report": report})
        elif not args.wait:
            st = report.get("status") or {}
            print(f"{args.job_id}: {state} (IRI state {st.get('state')}, exit_code {st.get('exit_code')})")
        if args.wait and state != jobs.FINISHED:
            raise JobFailed(f"job {args.job_id} ended {state}")
        return EXIT_OK
    if args.action == "cancel":
        api.job_cancel(args.site, args.job_id)
        print(f"cancelled {args.job_id}")
        return EXIT_OK
    if args.action == "cleanup":
        for path, what in api.job_cleanup(args.site, args.run_id).items():
            print(f"{path}: {what}")
        return EXIT_OK
    if args.action == "list":
        rows = [(j["job_id"], j["name"], j["iri_state"], j["state"])
                for j in api.job_list(args.site, historical=args.historical, limit=args.limit)]
        print(table(rows, ["JOB_ID", "NAME", "IRI_STATE", "STATE"]))
        return EXIT_OK
    raise ConfigError(f"unknown job action {args.action}")


# ---------------------------------------------------------------------- fs
def cmd_fs(args, api):
    kw = dict(resource_id=args.resource_id)
    if args.action == "ls":
        print_json(api.fs_ls(args.site, args.path, show_hidden=args.all, **kw))
    elif args.action == "stat":
        print_json(api.fs_stat(args.site, args.path, **kw))
    elif args.action == "mkdir":
        print_json(api.fs_mkdir(args.site, args.path, parents=args.parents, **kw))
    elif args.action == "chmod":
        print_json(api.fs_chmod(args.site, args.path, args.mode, **kw))
    elif args.action == "rm":
        removed = api.fs_rm(args.site, args.path, force=args.force, **kw)
        print(f"{args.path}: {'removed' if removed else 'already gone'}")
    return EXIT_OK


# ---------------------------------------------------------------------- staging
def cmd_https_stage(args, api):
    if args.action == "stagein":
        for r in api.https_stagein(args.site, args.files, args.to, mkdir=args.mkdir, executable=args.executable):
            print(f"uploaded {r}")
        return EXIT_OK
    if args.job_id:
        res = api.https_stageout_job_logs(args.site, args.job_id, args.to, run_id=args.run_id, overwrite=args.overwrite)
    else:
        if not args.files:
            raise ConfigError("give REMOTE_FILE... or --job-id")
        res = api.https_stageout(args.site, args.files, args.to, overwrite=args.overwrite)
    return print_results(res)


def cmd_e2e_stage(args, api):
    if args.action in ("stagein", "stageout"):
        pairs = []
        if args.paths:
            if len(args.paths) != 2:
                raise ConfigError("give exactly SRC DST")
            pairs = [tuple(args.paths) + ((args.scope, args.name) if args.name else ())]
        if (args.scope or args.name) and not args.rucio_deterministic:
            raise ConfigError("--scope/--name need --rucio-deterministic")
        if args.rucio_deterministic and not args.scope:
            raise ConfigError("--rucio-deterministic needs --scope SCOPE")
        if args.name and not pairs:
            raise ConfigError("--name needs one SRC DST")
        # a tracked transfer run: `monitor`, `daemon`, `runs`, `cancel` and `--resume` work on it
        r = api.e2e_submit(args.site, args.action, pairs, manifest=args.manifest, rucio=args.rucio,
                           endpoint=args.endpoint, sync_level=args.sync_level,
                           rucio_deterministic=args.rucio_deterministic, scope=args.scope, run_id=args.run_id,
                           validate=not args.no_validate, wait=not args.no_wait, echo=print)
        print_runs([r])
        if r["status"] == "active":
            print(f"next: globusiri monitor {r['run_id']}   (or keep `globusiri daemon` running)")
        return _exit_for(r)
    if args.action == "cleanup":
        r = api.e2e_cleanup(args.site, args.paths, manifest=args.manifest, rucio=args.rucio, force=args.force,
                            dry_run=args.dry_run, wait=False, rucio_deterministic=bool(args.scope), scope=args.scope)
        if args.dry_run:
            for item in r["items"]:
                print(f"would delete {r['endpoint']}:{item}")
            return EXIT_OK
        print(f"cleanup on {r['endpoint']}: {len(r['items'])} files, tasks {', '.join(r['task_ids'])}")
        if not args.no_wait:
            api.e2e_wait(r["task_ids"], on_update=lambda t, i: print(f"  {t}: {i['status']}"))
            print("done")
        return EXIT_OK
    if args.action == "task":
        def show():
            info = api.e2e_task(args.task_id, failures=args.failures)
            failures = info.pop("failures", None)
            print_json(info)
            if failures is not None:
                print_json(failures)
        if args.wait:
            try:
                api.e2e_wait(args.task_id)
            finally:
                show()
        else:
            show()
        return EXIT_OK
    if args.action == "cancel":
        print_json(api.e2e_cancel(args.task_id))
        return EXIT_OK
    raise ConfigError(f"unknown e2e action {args.action}")


# ---------------------------------------------------------------------- run
def cmd_run(args, api):
    kwargs = dict(wait=not args.no_wait, validate=not args.no_validate, interval=args.interval, echo=print)
    if args.resume:
        api.resume(args.resume, **kwargs)
        return EXIT_OK
    if not args.job:
        raise ConfigError("run needs --job JOB_FILE (or --resume RUN_ID)")
    api.run(args.job, args.site, set=args.set, run_id=args.run_id, stagein_manifest=args.stagein_manifest,
            stageout_manifest=args.stageout_manifest, **kwargs)
    return EXIT_OK


# ---------------------------------------------------------------------- non-blocking workflow
def _run_row(r):
    steps = r.get("steps") or {}
    cur = r.get("current_step") or "-"
    detail = r.get("step_state") or r.get("step_status") or ""
    if cur in steps:
        st = steps[cur]
        detail = st.get("state") or st.get("status") or ""
    err = (r.get("last_error") or {}).get("error") or r.get("message")
    return [r.get("run_id"), r.get("site"), r.get("status"), cur, detail,
            (steps.get("submit") or {}).get("job_id") or r.get("job_id"),
            (r.get("updated_at") or "")[:19], err or ""]


RUN_HEADERS = ["RUN_ID", "SITE", "STATUS", "STEP", "STEP_STATE", "JOB_ID", "UPDATED", "MESSAGE"]


def print_runs(runs, as_json=False):
    if as_json:
        print_json(runs)
    elif runs:
        print(table([_run_row(r) for r in runs], RUN_HEADERS))
    else:
        print("no runs")


def _exit_for(r):
    from .workflow import DOWNLOAD_FAILED, FAILED, UPLOAD_FAILED

    return JobFailed.exit_code if r["status"] in (FAILED, UPLOAD_FAILED, DOWNLOAD_FAILED) else EXIT_OK


def _retry_hint(runs):
    from .workflow import RETRYABLE

    for r in runs:
        if r.get("status") in RETRYABLE:
            print(f"{r['run_id']}: {r['status']}; retry the {RETRYABLE[r['status']].replace('https_', '')} "
                  f"with: globusiri monitor {r['run_id']}")


def cmd_submit(args, api):
    if args.resume:
        if args.job:
            raise ConfigError("give --job JOB_FILE (new run) or --resume RUN_ID (existing run), not both")
        api.reopen(args.resume, validate=False if args.no_validate else None)
        res = api.monitor([args.resume], echo=None if args.json else print)[args.resume]
        if not isinstance(res, dict):  # e.g. busy: another monitor holds the run
            print(f"{args.resume}: {res}", file=sys.stderr)
            res = api.run_state(args.resume)
        r = res
    elif not args.job:
        raise ConfigError("submit needs --job JOB_FILE (or --resume RUN_ID)")
    else:
        r = _submit_new(args, api)
    if args.json:
        print_json(r)
    else:
        print_runs([r])
        if r["status"] == "active":
            print(f"next: globusiri monitor {r['run_id']}   (or keep `globusiri daemon` running)")
        _retry_hint([r])
    return _exit_for(r)


def _submit_new(args, api):
    return api.submit(args.job, args.site, set=args.set, inputs=args.input, run_id=args.run_id,
                   stagein_manifest=args.stagein_manifest, stageout_manifest=args.stageout_manifest,
                      validate=not args.no_validate, max_attempts=args.max_attempts,
                      echo=None if args.json else print)


def cmd_monitor(args, api):
    res = api.monitor(args.run_ids or None, resume=args.resume, echo=None if args.json else print)
    if args.json:
        print_json(res)
        return EXIT_OK
    runs = [r for r in res.values() if isinstance(r, dict)]
    print_runs(runs)
    _retry_hint(runs)
    for rid, r in res.items():
        if not isinstance(r, dict):
            print(f"{rid}: {r}", file=sys.stderr)
    return EXIT_OK


def cmd_daemon(args, api):
    import signal

    stopping = {"flag": False}

    def _stop(signum, frame):
        stopping["flag"] = True
        log.info("daemon: signal %s, stopping after this pass", signum)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    print(f"globusiri daemon: WORK_DIR {api.config.paths.root}, every {args.interval}s (SIGTERM/Ctrl-C to stop)")
    api.daemon(interval=args.interval, echo=print, stop=lambda: stopping["flag"],
               max_passes=1 if args.once else None)
    return EXIT_OK


def cmd_runs(args, api):
    if args.run_id:
        if args.reindex:
            api.runs(summary=True, reindex=True)
        r = api.run_state(args.run_id)
        if args.json:
            print_json(r)
            return EXIT_OK
        print_runs([r])
        print()
        print(table([[k, v.get("status"), v.get("state") or v.get("job_id") or ",".join(v.get("task_ids") or []),
                      v.get("error") or v.get("reason") or ""] for k, v in r["steps"].items()],
                    ["STEP", "STATUS", "DETAIL", "NOTE"]))
        return EXIT_OK
    print_runs(api.runs(status=args.status, summary=not args.json, reindex=args.reindex), args.json)
    return EXIT_OK


def cmd_cancel(args, api):
    r = api.cancel(args.run_id, echo=print)
    print_runs([r])
    return EXIT_OK


# ---------------------------------------------------------------------- parser
def build_parser():
    p = argparse.ArgumentParser(prog="globusiri", description="Run HPC jobs through IRI and move files with Globus.")
    p.add_argument("--version", action="version", version=f"globusiri {__version__}")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="site config (default: $GLOBUSIRI_CONFIG or <WORK_DIR>/config/sites.yaml)")
    common.add_argument("--debug", action="store_true", help="verbose, redacted request traces")
    sub = p.add_subparsers(dest="group", required=True, metavar="COMMAND")

    s = sub.add_parser("init", parents=[common], help="create WORK_DIR, config/sites.yaml and example job files")
    s.add_argument("--force", action="store_true", help="overwrite existing files")
    s.add_argument("--sites", help="comma-separated sites to keep from the example config")
    s.add_argument("--work-dir", help=argparse.SUPPRESS)

    sub.add_parser("paths", parents=[common], help="show resolved WORK_DIR, config, token and log locations")

    s = sub.add_parser("validate", parents=[common], help="validate iri/https/e2e tokens (live)")
    s.add_argument("sites", nargs="*", metavar="SITE")
    s.add_argument("--job", help="check only the tokens this job file needs")
    s.add_argument("--kinds", help="comma-separated subset of iri,https,e2e")
    s.add_argument("--offline", action="store_true", help="file checks only, no network")
    s.add_argument("--write-test", action="store_true", help="HTTPS: also PUT+GET a small file in remote.work_dir")
    s.add_argument("--panda", action="store_true", help="compare PanDA secrets with local files")
    s.add_argument("--min-lifetime", help="WARN if a token expires sooner (e.g. 6h)")
    s.add_argument("--strict", action="store_true", help="exit non-zero on WARN too")
    s.add_argument("--json", action="store_true", help="one JSON object per check")

    for kind in KINDS:
        g = sub.add_parser(kind, help=f"{kind} token and file commands")
        gs = g.add_subparsers(dest="action", required=True, metavar="ACTION")
        t = gs.add_parser("login", parents=[common], help="interactive Globus login; writes the refresh token")
        t.add_argument("site", nargs="?", metavar="SITE")
        t.add_argument("--force", action="store_true", help="log in again even if a refresh token exists")
        t.add_argument("--prompt-login", action="store_true", help="force Globus to ask for credentials")
        t.add_argument("--import", dest="import_file", help="import an existing refresh token file instead of logging in")
        t = gs.add_parser("refresh", parents=[common], help="refresh token -> access token (never prompts)")
        t.add_argument("site", nargs="?", metavar="SITE")
        t.add_argument("--all", action="store_true", help="every site")
        t.add_argument("--min-lifetime", help="skip if the current token lives longer than this (e.g. 6h)")
        t.add_argument("--upload-to-panda", action="store_true", help="also upload to the PanDA secret if it changed")
        t.add_argument("--no-validate", action="store_true", help="skip the IRI validation call after refresh")
        for action, helptext in (("upload_to_panda", "access token -> PanDA user secret"),
                                 ("download_from_panda", "PanDA user secret -> access token file"),
                                 ("status", "offline: files, permissions, expiry")):
            t = gs.add_parser(action, parents=[common], help=helptext)
            t.add_argument("site", nargs="?", metavar="SITE")
            t.add_argument("--all", action="store_true", help="every site")

        if kind == "https":
            t = gs.add_parser("stagein", parents=[common], help="upload local files to the HPC over HTTPS")
            t.add_argument("site", metavar="SITE")
            t.add_argument("files", nargs="+", metavar="LOCAL_FILE")
            t.add_argument("--to", required=True, metavar="REMOTE_DIR", help="HPC directory (mount path)")
            t.add_argument("--mkdir", action="store_true", help="create REMOTE_DIR via IRI first")
            t.add_argument("--executable", action="append", metavar="NAME", help="chmod 0755 this uploaded file via IRI")
            t = gs.add_parser("stageout", parents=[common], help="download files from the HPC over HTTPS")
            t.add_argument("site", metavar="SITE")
            t.add_argument("files", nargs="*", metavar="REMOTE_FILE")
            t.add_argument("--to", required=True, metavar="LOCAL_DIR")
            t.add_argument("--job-id", help="download this IRI job's stdout/stderr")
            t.add_argument("--run-id", help="with --job-id: fallback stdout/stderr location <log_dir>/<run>/")
            t.add_argument("--overwrite", action="store_true", help="re-download files that exist locally")
        if kind == "e2e":
            for action, ep_flag in (("stagein", "--src-endpoint"), ("stageout", "--dst-endpoint")):
                t = gs.add_parser(action, parents=[common], help=f"Globus Transfer {action}")
                t.add_argument("site", metavar="SITE")
                t.add_argument("paths", nargs="*", metavar="SRC DST")
                t.add_argument("--manifest", help="file of 'SRC DST' lines")
                t.add_argument("--rucio", action="append", metavar="SCOPE:NAME",
                               help="repeatable; only builds the Rucio deterministic RSE path from the DID "
                                    "(no Rucio catalog lookup, replica check or registration)")
                t.add_argument(ep_flag, dest="endpoint", metavar="LABEL", help="storage endpoint label (default from site)")
                t.add_argument("--rucio-deterministic", action="store_true",
                               help="DST (and each manifest DST) is a directory; files go to their Rucio "
                                    "deterministic path below it (needs --scope)")
                t.add_argument("--scope", metavar="SCOPE", help="Rucio scope for --rucio-deterministic")
                t.add_argument("--name", metavar="NAME",
                               help="with --rucio-deterministic and one SRC DST: the Rucio file name "
                                    "(default: SRC's basename)")
                t.add_argument("--sync-level", choices=["exists", "size", "mtime", "checksum"])
                t.add_argument("--run-id", help="run ID of this transfer (default: generated)")
                t.add_argument("--no-validate", action="store_true", help="skip the e2e token validation")
                t.add_argument("--no-wait", action="store_true",
                               help="submit and return; `globusiri monitor` (or the daemon) checks the transfer")
            t = gs.add_parser("cleanup", parents=[common],
                              help="delete stage-in files on the HPC endpoint (Globus delete task)")
            t.add_argument("site", metavar="SITE")
            t.add_argument("paths", nargs="*", metavar="PATH", help="HPC file path (mount path)")
            t.add_argument("--manifest", help="stage-in manifest; its DST column is deleted")
            t.add_argument("--scope", metavar="SCOPE",
                           help="the stage-in used --rucio-deterministic --scope SCOPE with this manifest")
            t.add_argument("--rucio", action="append", metavar="SCOPE:NAME",
                           help="repeatable; deletes the Rucio deterministic path under e2e.base_path")
            t.add_argument("--force", action="store_true",
                           help="allow paths outside remote.work_dir, remote.log_dir and e2e.base_path")
            t.add_argument("--dry-run", action="store_true", help="print what would be deleted")
            t.add_argument("--no-wait", action="store_true")
            t = gs.add_parser("task", parents=[common], help="show a transfer task")
            t.add_argument("task_id", metavar="TASK_ID")
            t.add_argument("--wait", action="store_true")
            t.add_argument("--failures", action="store_true")
            t = gs.add_parser("cancel", parents=[common], help="cancel a transfer task")
            t.add_argument("task_id", metavar="TASK_ID")

    g = sub.add_parser("job", help="IRI jobs")
    gs = g.add_subparsers(dest="action", required=True, metavar="ACTION")
    t = gs.add_parser("submit", parents=[common], help="render, upload and submit a job file")
    t.add_argument("site", nargs="?", metavar="SITE")
    t.add_argument("--job", required=True, metavar="JOB_FILE")
    t.add_argument("--run-id")
    t.add_argument("--set", action="append", metavar="KEY=VALUE", help="override a job key, e.g. resources.n_node=4")
    t.add_argument("--input", action="append", metavar="NAME=LOCAL", help="add/override an input file")
    t.add_argument("--dry-run", action="store_true", help="print the rendered script and job_spec only")
    t.add_argument("--no-validate", action="store_true")
    t = gs.add_parser("status", parents=[common], help="job state")
    t.add_argument("site", metavar="SITE")
    t.add_argument("job_id", metavar="JOB_ID")
    t.add_argument("--wait", action="store_true")
    t.add_argument("--interval", type=int, default=60)
    t.add_argument("--json", action="store_true")
    t = gs.add_parser("cancel", parents=[common], help="cancel a job")
    t.add_argument("site", metavar="SITE")
    t.add_argument("job_id", metavar="JOB_ID")
    t = gs.add_parser("cleanup", parents=[common],
                      help="remove a run's remote directory (and log directory) via IRI rm")
    t.add_argument("site", metavar="SITE")
    t.add_argument("run_id", metavar="RUN_ID")
    t = gs.add_parser("list", parents=[common], help="list jobs")
    t.add_argument("site", metavar="SITE")
    t.add_argument("--historical", action="store_true")
    t.add_argument("--limit", type=int)

    g = sub.add_parser("fs", help="IRI filesystem helpers")
    gs = g.add_subparsers(dest="action", required=True, metavar="ACTION")
    for action in ("ls", "stat", "mkdir", "chmod", "rm"):
        t = gs.add_parser(action, parents=[common], help="remove a file or directory" if action == "rm" else None)
        t.add_argument("site", metavar="SITE")
        t.add_argument("path", metavar="PATH")
        if action == "chmod":
            t.add_argument("mode", metavar="MODE")
        if action == "mkdir":
            t.add_argument("--parents", action="store_true")
        if action == "ls":
            t.add_argument("--all", action="store_true", help="show hidden files")
        if action == "rm":
            t.add_argument("--force", action="store_true",
                           help="allow paths outside remote.work_dir, remote.log_dir and e2e.base_path")
        t.add_argument("--resource-id", help="filesystem resource (default: remote.work_dir_resource_id)")

    s = sub.add_parser("run", parents=[common], help="end-to-end run of a job file")
    s.add_argument("site", nargs="?", metavar="SITE")
    s.add_argument("--job", metavar="JOB_FILE")
    s.add_argument("--resume", metavar="RUN_ID")
    s.add_argument("--run-id")
    s.add_argument("--set", action="append", metavar="KEY=VALUE")
    s.add_argument("--stagein-manifest")
    s.add_argument("--stageout-manifest")
    s.add_argument("--no-validate", action="store_true")
    s.add_argument("--no-wait", action="store_true")
    s.add_argument("--interval", type=int, default=60, help="job poll interval in seconds")

    s = sub.add_parser("submit", parents=[common],
                       help="start a workflow run without waiting; `monitor` or `daemon` drives it on")
    s.add_argument("site", nargs="?", metavar="SITE")
    s.add_argument("--job", metavar="JOB_FILE")
    s.add_argument("--resume", metavar="RUN_ID",
                   help="retry a failed run: failed steps go back to pending, done steps are kept")
    s.add_argument("--run-id")
    s.add_argument("--set", action="append", metavar="KEY=VALUE")
    s.add_argument("--input", action="append", metavar="NAME=LOCAL", help="add/override an input file")
    s.add_argument("--stagein-manifest")
    s.add_argument("--stageout-manifest")
    s.add_argument("--no-validate", action="store_true")
    s.add_argument("--max-attempts", type=int, default=5,
                   help="passes a step may fail with a transient error before the run fails (default 5)")
    s.add_argument("--json", action="store_true")

    s = sub.add_parser("monitor", parents=[common],
                       help="one pass: check each active run's current step and trigger the next")
    s.add_argument("run_ids", nargs="*", metavar="RUN_ID",
                   help="default: every active run. A run given here that ended upload_failed or "
                        "download_failed redoes the failed upload/download, then carries on")
    s.add_argument("--resume", action="store_true",
                   help="first reopen the given failed runs (failed steps go back to pending)")
    s.add_argument("--json", action="store_true")

    s = sub.add_parser("daemon", parents=[common], help="run `monitor` in a loop")
    s.add_argument("--interval", type=int, default=60, help="seconds between passes (default 60)")
    s.add_argument("--once", action="store_true", help="one pass, then exit")

    s = sub.add_parser("runs", parents=[common], help="list workflow runs, or show one")
    s.add_argument("run_id", nargs="?", metavar="RUN_ID")
    s.add_argument("--status", choices=["active", "finished", "failed", "cancelled", "upload_failed",
                                        "download_failed"])
    s.add_argument("--reindex", action="store_true", help="rebuild the central index from saved run states")
    s.add_argument("--json", action="store_true")

    s = sub.add_parser("cancel", parents=[common], help="cancel a run's IRI job and running transfers")
    s.add_argument("run_id", metavar="RUN_ID")
    return p


def dispatch(args, paths):
    api = GlobusIRI(getattr(args, "config", None), work_dir=paths.root)
    group, action = args.group, getattr(args, "action", None)
    if group == "init":
        return cmd_init(args, api)
    if group == "paths":
        return cmd_paths(args, api)
    if group == "validate":
        return cmd_validate(args, api)
    if group == "job":
        return cmd_job(args, api)
    if group == "fs":
        return cmd_fs(args, api)
    if group == "run":
        return cmd_run(args, api)
    if group in ("submit", "monitor", "daemon", "runs", "cancel"):
        return globals()[f"cmd_{group}"](args, api)
    if group == "https" and action in ("stagein", "stageout"):
        return cmd_https_stage(args, api)
    if group == "e2e" and action in ("stagein", "stageout", "cleanup", "task", "cancel"):
        return cmd_e2e_stage(args, api)
    return cmd_token(args, api)


def main(argv=None):
    args = build_parser().parse_args(argv)
    paths = Paths()
    setup_logging(paths, getattr(args, "debug", False))
    try:
        return dispatch(args, paths)
    except GlobusIriError as exc:
        print(f"error: {exc}", file=sys.stderr)
        log.info("exit %s: %s", exc.exit_code, exc)
        return exc.exit_code
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
