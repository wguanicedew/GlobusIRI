#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Globus E2E (endpoint-to-endpoint) Transfer staging, globus-sdk v4.

stagein  = storage endpoint -> HPC endpoint (before the job)
stageout = HPC endpoint -> storage endpoint (after the job)
cleanup  = Globus delete of stage-in files on the HPC endpoint

Every path is mapped through its own endpoint's path_map (HPC mount path -> Globus
path). Storage endpoints normally have no map, so their paths pass through unchanged.
"""

import hashlib
import logging
import posixpath
import time
from dataclasses import dataclass, field

import globus_sdk

from ..errors import ConfigError, JobFailed

log = logging.getLogger("globusiri.e2e")

TERMINAL = ("SUCCEEDED", "FAILED")


def rucio_path(base, scope, name):
    """Rucio deterministic path (same as harvester mover_utils.construct_file_path)."""
    digest = hashlib.md5(f"{scope}:{name}".encode("latin_1")).hexdigest()
    return posixpath.join(base.rstrip("/") or "/", scope.replace(".", "/"), digest[0:2], digest[2:4], name)


def deterministic_dst(src, dst, *, deterministic=False, scope=None, name=None):
    """The destination file of one transfer.

    deterministic false: DST is the destination file, used as given. deterministic true: DST is
    a directory (a Rucio RSE base such as .../atlasdatadisk/rucio) and the file goes to its Rucio
    deterministic path rucio_path(DST, scope, name), name defaulting to SRC's basename.
    """
    if not deterministic:
        if name:
            raise ConfigError(f"name '{name}' for {src} needs rucio_deterministic")
        return dst
    if not scope:
        raise ConfigError(f"rucio_deterministic for {src} needs a scope")
    name = name or posixpath.basename(str(src).rstrip("/"))
    for what, value in (("scope", scope), ("name", name)):
        if "/" in value or ":" in value:
            raise ConfigError(f"invalid Rucio {what} '{value}' for {src}")
    return rucio_path(dst, scope, name)


def split_did(text):
    """'SCOPE' or 'SCOPE:NAME' -> (scope, name or None)."""
    scope, _, name = str(text).partition(":")
    return scope, (name or None)


def resolve_pairs(pairs, *, deterministic=False, scope=None):
    """Transfer entries -> [(SRC, final DST)].

    Entries:
      (SRC, DST)                  uses the defaults `deterministic` and `scope`
      (SRC, DST, SCOPE[, NAME])   Rucio deterministic, with this scope (and name)
      {src, dst[, rucio_deterministic][, scope][, name]}   each key overrides the default
    A Rucio-deterministic entry's DST is a directory; see deterministic_dst.
    """
    out = []
    for item in pairs or []:
        if isinstance(item, dict):
            src, dst, name = item.get("src"), item.get("dst"), item.get("name")
            det = item.get("rucio_deterministic", deterministic)
            if not isinstance(det, bool):
                raise ConfigError(f"rucio_deterministic must be true or false, got: {item}")
            s = item.get("scope") or scope
        else:
            item = tuple(item)
            if len(item) not in (2, 3, 4):
                raise ConfigError(f"transfer entry must be (SRC, DST[, SCOPE[, NAME]]), got: {item}")
            src, dst = item[:2]
            if len(item) > 2:
                det, s, name = True, item[2], (item[3] if len(item) > 3 else None)
            else:
                det, s, name = deterministic, scope, None
        if not src or not dst:
            raise ConfigError(f"transfer entry needs src and dst, got: {item}")
        out.append((src, deterministic_dst(src, dst, deterministic=det, scope=s, name=name)))
    return out


def read_manifest(path, *, deterministic=False, scope=None):
    """Lines 'SRC DST' or 'SRC DST SCOPE[:NAME]' -> [(SRC, final DST)].

    A third column puts that line's file at its Rucio deterministic path below DST; other
    lines follow the defaults `deterministic` and `scope` (see resolve_pairs).
    """
    items = []
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) not in (2, 3):
                raise ConfigError(f"{path}:{n}: expected 'SRC DST [SCOPE[:NAME]]', got: {line}")
            entry = tuple(parts[:2]) + (split_did(parts[2]) if len(parts) == 3 else ())
            items += resolve_pairs([entry], deterministic=deterministic, scope=scope)
    return items


@dataclass
class Plan:
    direction: str
    src: object  # Endpoint
    dst: object  # Endpoint
    items: list = field(default_factory=list)  # (src_globus_path, dst_globus_path)


def plan(config, site, direction, *, manifest=None, rucio=(), pairs=(), endpoint_label=None,
         deterministic=False, scope=None):
    """Build a transfer plan. HPC-side paths are given as HPC mount paths.

    deterministic/scope: defaults for manifest and pair entries. A Rucio-deterministic entry's
    DST is a directory and the file goes to its Rucio deterministic path (see resolve_pairs).
    """
    if direction not in ("stagein", "stageout"):
        raise ValueError(direction)
    hpc = site.e2e_endpoint()
    if direction == "stagein":
        storage_label = endpoint_label or site.stagein.get("src_endpoint")
        storage_path = site.stagein.get("src_path")
    else:
        storage_label = endpoint_label or site.stageout.get("dst_endpoint")
        storage_path = site.stageout.get("dst_path")
    if not storage_label:
        raise ConfigError(f"sites.{site.name}.e2e.{direction} has no storage endpoint; pass one explicitly")
    storage = config.e2e.endpoint(storage_label)
    src, dst = (storage, hpc) if direction == "stagein" else (hpc, storage)

    raw = []
    if manifest:
        raw += read_manifest(manifest, deterministic=deterministic, scope=scope)
    raw += resolve_pairs(pairs, deterministic=deterministic, scope=scope)
    for did in rucio or []:
        if ":" not in did:
            raise ConfigError(f"rucio DID must be SCOPE:NAME, got '{did}'")
        scope, name = did.split(":", 1)
        if not site.e2e_base_path or not storage_path:
            raise ConfigError(f"sites.{site.name}.e2e needs base_path and {direction} storage path for --rucio")
        storage_file = rucio_path(storage_path, scope, name)
        hpc_file = rucio_path(site.e2e_base_path, scope, name)
        raw.append((storage_file, hpc_file) if direction == "stagein" else (hpc_file, storage_file))
    if not raw:
        raise ConfigError("nothing to transfer: give a manifest, --rucio, or SRC DST")

    p = Plan(direction, src, dst)
    p.items = [(src.path_map.to_globus(s), dst.path_map.to_globus(d)) for s, d in raw]
    return p


def stagein_hpc_paths(site, *, manifest=None, rucio=(), paths=(), deterministic=False, scope=None):
    """HPC-side (mount) paths that a stage-in wrote: manifest DST column (resolved like the
    stage-in, see read_manifest), Rucio paths under e2e.base_path, and any explicit paths."""
    out = []
    if manifest:
        out += [dst for _, dst in read_manifest(manifest, deterministic=deterministic, scope=scope)]
    for did in rucio or []:
        if ":" not in did:
            raise ConfigError(f"rucio DID must be SCOPE:NAME, got '{did}'")
        if not site.e2e_base_path:
            raise ConfigError(f"sites.{site.name}.e2e needs base_path for --rucio")
        scope, name = did.split(":", 1)
        out.append(rucio_path(site.e2e_base_path, scope, name))
    out += list(paths or [])
    return out


@dataclass
class DeletePlan:
    endpoint: object  # the site's HPC Endpoint
    items: list = field(default_factory=list)  # globus paths


def cleanup_plan(site, *, manifest=None, rucio=(), paths=(), force=False, deterministic=False, scope=None):
    """Plan a Globus delete of stage-in files on the HPC endpoint.

    Paths must be below the site's remote.work_dir, remote.log_dir or e2e.base_path
    unless force is set.
    """
    from ..cleanup import check_under, site_roots

    hpc_paths = stagein_hpc_paths(site, manifest=manifest, rucio=rucio, paths=paths, deterministic=deterministic,
                                  scope=scope)
    if not hpc_paths:
        raise ConfigError("nothing to clean up: give a manifest, --rucio, or PATH")
    roots = site_roots(site)
    hpc = site.e2e_endpoint()
    items = []
    for path in hpc_paths:
        g = hpc.path_map.to_globus(check_under(path, roots, force=force, what="stage-in file"))
        if g not in items:
            items.append(g)
    return DeletePlan(hpc, items)


def submit_delete(transfer, config, p, *, label=None):
    """Submit Globus delete tasks (files only, missing files ignored). Returns task ids."""
    task_ids = []
    batch = config.e2e.max_files_per_task
    for i in range(0, len(p.items), batch):
        chunk = p.items[i:i + batch]
        ddata = globus_sdk.DeleteData(p.endpoint.id, label=(label or "globusiri-cleanup")[:128],
                                      recursive=False, ignore_missing=True)
        for path in chunk:
            ddata.add_item(path)
        result = transfer.call(f"submit delete on {p.endpoint.label}", lambda tc: tc.submit_delete(ddata))
        task_ids.append(result["task_id"])
        log.info("submitted delete task %s (%d files) on %s", result["task_id"], len(chunk), p.endpoint.label)
    return task_ids


def submit(transfer, config, p, *, label=None, sync_level=None, verify_checksum=None):
    """Submit the plan in batches of e2e.max_files_per_task. Returns task ids."""
    e2e = config.e2e
    task_ids = []
    batch = e2e.max_files_per_task
    for i in range(0, len(p.items), batch):
        chunk = p.items[i:i + batch]
        tdata = globus_sdk.TransferData(
            p.src.id, p.dst.id,
            label=(label or f"globusiri-{p.direction}")[:128],
            sync_level=sync_level or e2e.sync_level,
            verify_checksum=e2e.verify_checksum if verify_checksum is None else verify_checksum,
        )
        for s, d in chunk:
            tdata.add_item(s, d)
        result = transfer.call(f"submit {p.direction} transfer {p.src.label}->{p.dst.label}",
                               lambda tc: tc.submit_transfer(tdata))
        task_ids.append(result["task_id"])
        log.info("submitted %s task %s (%d files)", p.direction, result["task_id"], len(chunk))
    return task_ids


def task_info(transfer, task_id):
    t = transfer.call(f"get task {task_id}", lambda tc: tc.get_task(task_id))
    return {k: t.get(k) for k in ("task_id", "status", "label", "files", "files_transferred", "files_skipped",
                                   "faults", "nice_status", "nice_status_details", "source_endpoint_display_name",
                                   "destination_endpoint_display_name", "completion_time")}


def task_failures(transfer, task_id, limit=50):
    out = []
    resp = transfer.call(f"skipped errors of {task_id}", lambda tc: tc.task_skipped_errors(task_id))
    for item in resp:
        out.append({"source": item.get("source_path"), "destination": item.get("destination_path"),
                    "error": item.get("error_code") or item.get("error_details")})
        if len(out) >= limit:
            break
    return out


def wait(transfer, task_ids, *, poll_interval=30, timeout=86400, on_update=None):
    """Wait for all tasks. Raises JobFailed if any task fails or the timeout passes."""
    start = time.time()
    pending = list(task_ids)
    results = {}
    while pending:
        for tid in list(pending):
            info = task_info(transfer, tid)
            if on_update:
                on_update(tid, info)
            if info["status"] in TERMINAL:
                results[tid] = info
                pending.remove(tid)
        if not pending:
            break
        if time.time() - start > timeout:
            raise JobFailed(f"transfer tasks {', '.join(pending)} not done after {timeout}s")
        time.sleep(poll_interval)
    failed = [tid for tid, info in results.items() if info["status"] != "SUCCEEDED"]
    if failed:
        details = []
        for tid in failed:
            info = results[tid]
            details.append(f"{tid}: {info.get('nice_status') or info['status']} {info.get('nice_status_details') or ''}".strip())
        raise JobFailed("transfer failed: " + "; ".join(details))
    return results


def cancel(transfer, task_id):
    return transfer.call(f"cancel task {task_id}", lambda tc: tc.cancel_task(task_id))
