#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""Atomic, private file writes and token redaction helpers."""

import json
import os
import re
import stat
import tempfile
from pathlib import Path

import yaml

_SECRET_KEYS = ("access_token", "refresh_token", "id_token")


def _ensure_private_dir(path):
    path.mkdir(parents=True, exist_ok=True)
    if path.name in ("iri", "https", "e2e", "tokens"):
        os.chmod(path, 0o700)


def write_private(path, content):
    """Write text atomically with mode 0600 (temp file in the same dir + os.replace)."""
    path = Path(path)
    _ensure_private_dir(path.parent)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_yaml_private(path, data):
    write_private(path, yaml.safe_dump(data, sort_keys=False, default_flow_style=False))


def write_json_private(path, data):
    write_private(path, json.dumps(data, indent=2) + "\n")


def read_yaml(path):
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data if isinstance(data, dict) else {}


def read_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def file_mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def is_private(path):
    """True if group/other have no access."""
    return file_mode(path) & 0o077 == 0


def redact(obj):
    """Return a copy of obj with token values replaced by '<redacted>'."""
    if isinstance(obj, dict):
        return {k: ("<redacted>" if k in _SECRET_KEYS and v else redact(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    if isinstance(obj, str):
        return redact_text(obj)
    return obj


_BEARER = re.compile(r"(Bearer\s+)[A-Za-z0-9._\-~+/=]+")


def redact_text(text):
    return _BEARER.sub(r"\1<redacted>", text)
