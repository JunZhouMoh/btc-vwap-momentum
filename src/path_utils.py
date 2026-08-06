#!/usr/bin/env python3
"""
Runtime path helpers.

On Railway we persist runtime artifacts to a mounted volume (default: /data).
Local development keeps existing relative paths unchanged.
"""

from __future__ import annotations

import os
from pathlib import Path

_JSON_SUFFIXES = {".json", ".jsonl"}


def is_railway_runtime() -> bool:
    """Detect whether the process is running on Railway."""
    return bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_PROJECT_ID"))


def data_mount_root() -> Path:
    """Return the persistent mount root used for runtime artifacts."""
    raw = (os.getenv("DATA_DIR") or "/data").strip()
    return Path(raw)


def resolve_runtime_path(path: str | Path, *, json_only: bool = False) -> Path:
    """
    Resolve runtime output path.

    - Non-Railway: returns original relative/absolute path
    - Railway: relative paths are rooted under DATA_DIR (/data by default)
    - json_only=True: remap only .json/.jsonl paths
    """
    p = Path(path)
    if p.is_absolute() or not is_railway_runtime():
        return p

    if json_only and p.suffix.lower() not in _JSON_SUFFIXES:
        return p

    return data_mount_root() / p
