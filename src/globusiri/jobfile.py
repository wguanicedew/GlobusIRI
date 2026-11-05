"""Job files: one per kind of submission.

Merge order (later wins): sites.<site>.job_defaults < job file < --set key=value.
Dicts merge key by key; lists and scalars replace.
"""

import copy
from pathlib import Path

import yaml

from .config import parse_duration
from .errors import ConfigError

TOP_LEVEL_KEYS = {"site", "name", "template_file", "template_vars", "resources", "inputs", "panda_auth_origin", "staging", "cleanup"}
LAUNCHERS = (None, "single", "mpirun", "srun", "aprun", "jsrun")

DEFAULT_JOB = {
    "name": "globusiri",
    "template_vars": {},
    "resources": {
        "n_node": 1,
        "n_core_per_node": 1,
        "n_process_per_node": 1,
        "gpu_cores_per_process": 0,
        "memory": None,
        "duration": None,
        "launcher": None,
        "exclusive_node_use": True,
        "custom_attributes": {},
    },
    "inputs": {},
    "panda_auth_origin": "",
    "staging": {
        "e2e": {"enabled": False, "stagein": {"manifest": None, "rucio": []}, "stageout": {"manifest": None, "rucio": []}},
        "https_stageout": {"stdout_stderr": True, "files": []},
    },
    # after a successful run only; failed runs keep everything for debugging
    "cleanup": {"remote_run_dir": False, "e2e_stagein": False},
}


def deep_merge(base, override):
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def normalize_defaults(job_defaults):
    """Site job_defaults may use resource keys at top level (project, constraint, ...)."""
    out = {}
    for k, v in (job_defaults or {}).items():
        if k in TOP_LEVEL_KEYS:
            out[k] = v
        else:
            out.setdefault("resources", {})[k] = v
    return out


def parse_set(assignments):
    """['resources.n_node=4', 'name=x'] -> nested dict. Values are parsed as YAML scalars."""
    out = {}
    for item in assignments or []:
        if "=" not in item:
            raise ConfigError(f"--set expects key=value, got '{item}'")
        key, _, raw = item.partition("=")
        keys = [k for k in key.strip().split(".") if k]
        if not keys:
            raise ConfigError(f"--set has an empty key: '{item}'")
        try:
            value = yaml.safe_load(raw) if raw.strip() else None
        except yaml.YAMLError:
            value = raw
        cur = out
        for k in keys[:-1]:
            cur = cur.setdefault(k, {})
            if not isinstance(cur, dict):
                raise ConfigError(f"--set conflicts on '{key}'")
        cur[keys[-1]] = value
    return out


class Job:
    """A fully resolved job (defaults + job file + overrides)."""

    def __init__(self, data, *, source=None):
        self.data = data
        self.source = source
        self._check()

    # accessors
    @property
    def site(self):
        return self.data.get("site")

    @property
    def name(self):
        return self.data.get("name") or "globusiri"

    @property
    def template_file(self):
        return self.data.get("template_file")

    @property
    def template_vars(self):
        return self.data.get("template_vars") or {}

    @property
    def resources(self):
        return self.data.get("resources") or {}

    @property
    def inputs(self):
        return self.data.get("inputs") or {}

    @property
    def panda_auth_origin(self):
        return self.data.get("panda_auth_origin") or ""

    @property
    def e2e(self):
        return (self.data.get("staging") or {}).get("e2e") or {}

    @property
    def e2e_enabled(self):
        return bool(self.e2e.get("enabled"))

    @property
    def https_stageout(self):
        return (self.data.get("staging") or {}).get("https_stageout") or {}

    @property
    def cleanup(self):
        return self.data.get("cleanup") or {}

    @property
    def duration(self):
        return parse_duration(self.resources.get("duration"))

    def _check(self):
        r = self.resources
        if r.get("launcher") not in LAUNCHERS:
            raise ConfigError(f"resources.launcher must be one of {LAUNCHERS}, not '{r.get('launcher')}'")
        for key in ("n_node", "n_core_per_node", "n_process_per_node"):
            try:
                if int(r.get(key, 1)) < 1:
                    raise ValueError
            except (TypeError, ValueError):
                raise ConfigError(f"resources.{key} must be a positive integer") from None
        self.duration  # validates format
        if not self.e2e_enabled:
            for direction in ("stagein", "stageout"):
                spec = self.e2e.get(direction) or {}
                if spec.get("manifest") or spec.get("rucio"):
                    raise ConfigError(
                        f"staging.e2e.{direction} lists files but staging.e2e.enabled is false; "
                        "set enabled: true or remove the files"
                    )

    def wants_https_download(self):
        h = self.https_stageout
        return bool(h.get("stdout_stderr")) or bool(h.get("files"))

    def required_kinds(self, site):
        """Token kinds this job needs at this site, as {kind: reason}; missing kinds are not needed."""
        kinds = {"iri": "always required"}
        reasons = []
        if site.upload_method == "globus_https":
            reasons.append("site uploads via globus_https")
        if self.wants_https_download():
            reasons.append("job downloads files over HTTPS")
        if reasons:
            kinds["https"] = "; ".join(reasons)
        if self.e2e_enabled:
            kinds["e2e"] = "job enables staging.e2e"
        return kinds

    def skipped_kinds(self, site):
        need = self.required_kinds(site)
        out = {}
        if "https" not in need:
            out["https"] = "job does not upload or download over HTTPS"
        if "e2e" not in need:
            out["e2e"] = "job does not enable e2e"
        return out

    def snapshot(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(self.data, sort_keys=False), encoding="utf-8")


def load_job_data(path):
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"job file {path} must be a YAML mapping")
    unknown = set(data) - TOP_LEVEL_KEYS
    if unknown:
        raise ConfigError(f"job file {path} has unknown keys: {', '.join(sorted(unknown))}")
    return data


def resolve_job(config, job_ref=None, *, site_name=None, overrides=None, snapshot_data=None):
    """Build a Job from a job file (or a saved snapshot), the site defaults and --set overrides.

    Returns (job, site).
    """
    from .config import expand_env

    source = None
    if snapshot_data is not None:
        file_data = snapshot_data
    elif job_ref:
        source = config.paths.find_job_file(job_ref)
        file_data = load_job_data(source)
    else:
        file_data = {}
    override_data = parse_set(overrides) if isinstance(overrides, (list, tuple)) else (overrides or {})

    name = site_name or override_data.get("site") or file_data.get("site")
    if not name:
        raise ConfigError("no site given: pass SITE or set 'site' in the job file")
    site = config.site(name)

    data = deep_merge(DEFAULT_JOB, normalize_defaults(site.job_defaults))
    data = deep_merge(data, file_data)
    data = deep_merge(data, override_data)
    data["site"] = site.name
    return Job(expand_env(data), source=source), site
