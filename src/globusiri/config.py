"""Load and validate sites.yaml.

The site config describes sites and is not edited per job. Per-job settings live in
job files (see jobfile.py). Token locations come from paths.Paths by convention and
can be overridden with a top-level ``token_files`` mapping.
"""

import os
import posixpath
import re
from pathlib import Path

import yaml

from .errors import ConfigError
from .paths import Paths

DEFAULT_GLOBUS_CLIENT_ID = "165ae151-86aa-430a-8251-ce565e51998f"  # USATLAS_HPC_Globus
FACILITIES = ("nersc", "alcf")
UPLOAD_METHODS = ("iri", "globus_https")

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*$")
_UNITS = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value, default=None):
    """'1800' / 1800 / '30m' / '6h' / '2d' -> seconds (int). None -> default."""
    if value is None or value == "":
        return default
    if isinstance(value, (int, float)):
        return int(value)
    m = _DURATION.match(str(value))
    if not m:
        raise ConfigError(f"invalid duration '{value}' (use seconds or a value like 30m, 6h, 2d)")
    return int(float(m.group(1)) * _UNITS[m.group(2)])


def expand_env(obj):
    """Recursively expand ${VAR} in strings. Unset variables are left as-is."""
    if isinstance(obj, dict):
        return {k: expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


class PathMap:
    """Map HPC mount paths to Globus collection paths by longest matching prefix.

    An empty map means the two are identical (e.g. NERSC).
    """

    def __init__(self, mapping=None):
        items = []
        for hpc, glob in (mapping or {}).items():
            items.append((self._norm(hpc), self._norm(glob)))
        # longest prefix first
        self._to_globus = sorted(items, key=lambda kv: len(kv[0]), reverse=True)
        self._to_hpc = sorted(items, key=lambda kv: len(kv[1]), reverse=True)

    @staticmethod
    def _norm(p):
        p = posixpath.normpath("/" + str(p).strip("/")) if str(p).strip("/") else "/"
        return p

    @staticmethod
    def _swap(path, src, dst):
        path = posixpath.normpath(path)
        if src == "/":
            rest = path.lstrip("/")
        elif path == src:
            rest = ""
        elif path.startswith(src + "/"):
            rest = path[len(src) + 1:]
        else:
            return None
        return posixpath.join(dst, rest) if rest else dst

    def to_globus(self, hpc_path):
        for hpc, glob in self._to_globus:
            out = self._swap(hpc_path, hpc, glob)
            if out is not None:
                return out
        return hpc_path

    def to_hpc(self, globus_path):
        for hpc, glob in self._to_hpc:
            out = self._swap(globus_path, glob, hpc)
            if out is not None:
                return out
        return globus_path

    def __bool__(self):
        return bool(self._to_globus)

    def __repr__(self):
        return f"PathMap({dict(self._to_globus)})"


def str_list(value):
    """None | "a" | "a, b" | ["a", "b"] -> ["a", "b"]."""
    if not value:
        return []
    if isinstance(value, str):
        value = value.split(",")
    return [str(v).strip() for v in value if v and str(v).strip()]


class Endpoint:
    def __init__(self, label, raw):
        self.label = label
        raw = raw or {}
        self.id = raw.get("id")
        self.query = raw.get("query")
        self.pick = int(raw.get("pick", 0) or 0)
        self.data_access = bool(raw.get("data_access", False))
        self.path_map = PathMap(raw.get("path_map"))
        # identity domains whose login `e2e login` requires before issuing the token
        self.session_domains = str_list(raw.get("session_domains"))
        if not self.id and not self.query:
            raise ConfigError(f"e2e.endpoints.{label}: give 'id' and/or 'query'")


class E2EConfig:
    def __init__(self, raw):
        raw = raw or {}
        self.raw = raw
        self.panda_secret_key = raw.get("panda_secret_key")
        self.sync_level = raw.get("sync_level", "exists")
        if self.sync_level not in ("exists", "size", "mtime", "checksum"):
            raise ConfigError(f"e2e.sync_level must be exists|size|mtime|checksum, not '{self.sync_level}'")
        self.verify_checksum = bool(raw.get("verify_checksum", True))
        self.max_files_per_task = int(raw.get("max_files_per_task", 1000))
        self.poll_interval = parse_duration(raw.get("poll_interval"), 30)
        self.timeout = parse_duration(raw.get("timeout"), 86400)
        self.endpoints = {label: Endpoint(label, ep) for label, ep in (raw.get("endpoints") or {}).items()}

    def endpoint(self, label):
        try:
            return self.endpoints[label]
        except KeyError:
            raise ConfigError(f"unknown e2e endpoint '{label}' (known: {', '.join(self.endpoints) or 'none'})") from None


class Site:
    def __init__(self, name, raw, config):
        self.name = name
        self.raw = raw
        self.config = config
        self.facility = raw.get("facility")
        if self.facility not in FACILITIES:
            raise ConfigError(f"sites.{name}.facility must be one of {FACILITIES}, not '{self.facility}'")

        iri = raw.get("iri") or {}
        self.iri = iri
        self.base_url = (iri.get("base_url") or "").rstrip("/")
        self.resource_id = iri.get("resource_id")
        if not self.base_url or not self.resource_id:
            raise ConfigError(f"sites.{name}.iri needs base_url and resource_id")
        self.iri_panda_secret_key = iri.get("panda_secret_key")
        self.iri_validate = bool(iri.get("validate", True))
        self.iri_debug = bool(iri.get("debug", False))
        self.iri_session_domains = str_list(iri.get("session_domains"))

        remote = raw.get("remote") or {}
        self.work_dir = remote.get("work_dir")
        if not self.work_dir:
            raise ConfigError(f"sites.{name}.remote.work_dir is required")
        self.work_dir_resource_id = remote.get("work_dir_resource_id") or self.resource_id
        self.log_dir = remote.get("log_dir") or self.work_dir
        self.log_dir_resource_id = remote.get("log_dir_resource_id") or self.work_dir_resource_id
        self.upload_method = remote.get("upload_method", "iri")
        if self.upload_method not in UPLOAD_METHODS:
            raise ConfigError(f"sites.{name}.remote.upload_method must be one of {UPLOAD_METHODS}")

        fields = raw.get("unsupported_resource_fields") or []
        if isinstance(fields, str):
            fields = fields.split(",")
        self.unsupported_resource_fields = {f.strip() for f in fields if f and f.strip()}
        self.job_defaults = raw.get("job_defaults") or {}

        https = raw.get("https") or {}
        self.https = https
        self.https_collection_query = https.get("collection_query")
        self.https_collection_pick = int(https.get("collection_pick", 0) or 0)
        self.https_collection_id = https.get("collection_id")
        self.https_server = https.get("https_server")
        self.https_panda_secret_key = https.get("panda_secret_key")
        self.https_path_map = PathMap(https.get("path_map"))
        self.https_session_domains = str_list(https.get("session_domains"))

        e2e = raw.get("e2e") or {}
        self.e2e = e2e
        self.e2e_endpoint_label = e2e.get("endpoint")
        self.e2e_base_path = e2e.get("base_path")
        self.stagein = e2e.get("stagein") or {}
        self.stageout = e2e.get("stageout") or {}

    @property
    def has_https(self):
        return bool(self.https)

    @property
    def has_e2e(self):
        return bool(self.e2e_endpoint_label)

    def e2e_endpoint(self):
        if not self.e2e_endpoint_label:
            raise ConfigError(f"sites.{self.name}.e2e.endpoint is not set")
        return self.config.e2e.endpoint(self.e2e_endpoint_label)

    def remote_run_dir(self, run_id):
        return posixpath.join(self.work_dir, run_id)

    def remote_log_dir(self, run_id):
        return posixpath.join(self.log_dir, run_id)


class Config:
    def __init__(self, raw, *, path=None, paths=None):
        self.path = path
        self.paths = paths or Paths()
        raw = expand_env(raw or {})
        self.raw = raw
        defaults = raw.get("defaults") or {}
        self.globus_client_id = defaults.get("globus_client_id", DEFAULT_GLOBUS_CLIENT_ID)
        self.refresh_min_lifetime = parse_duration(defaults.get("refresh_min_lifetime"), 0)
        self.retries = int(defaults.get("retries", 3))
        self.e2e = E2EConfig(raw.get("e2e"))
        self.token_files = raw.get("token_files") or {}
        sites = raw.get("sites") or {}
        if not isinstance(sites, dict):
            raise ConfigError("'sites' must be a mapping")
        self.sites = {name: Site(name, s or {}, self) for name, s in sites.items()}
        for site in self.sites.values():
            if site.e2e_endpoint_label and site.e2e_endpoint_label not in self.e2e.endpoints:
                raise ConfigError(f"sites.{site.name}.e2e.endpoint '{site.e2e_endpoint_label}' is not in e2e.endpoints")
            for key, sect in (("stagein", "src_endpoint"), ("stageout", "dst_endpoint")):
                label = getattr(site, key).get(sect)
                if label and label not in self.e2e.endpoints:
                    raise ConfigError(f"sites.{site.name}.e2e.{key}.{sect} '{label}' is not in e2e.endpoints")

    def site(self, name):
        try:
            return self.sites[name]
        except KeyError:
            raise ConfigError(f"unknown site '{name}' (known: {', '.join(self.sites) or 'none'})") from None

    def token_file(self, key, default):
        """Token file path, honouring an optional ``token_files`` override."""
        override = self.token_files.get(key)
        return self.paths.resolve(override) if override else Path(default)

    def facilities(self):
        return sorted({s.facility for s in self.sites.values()})

    def sites_for_facility(self, facility):
        return [s for s in self.sites.values() if s.facility == facility]


def load_config(path=None, *, paths=None):
    paths = paths or Paths()
    cfg_path, source = paths.config_file(path)
    if not cfg_path.is_file():
        raise ConfigError(f"config file {cfg_path} not found (from {source})", fix="globusiri init")
    try:
        with open(cfg_path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"cannot parse {cfg_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{cfg_path} must be a YAML mapping")
    return Config(raw, path=cfg_path, paths=paths)
