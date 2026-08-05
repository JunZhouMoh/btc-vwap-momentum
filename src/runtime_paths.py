#!/usr/bin/env python3
"""Runtime file path helpers (local + Railway volume aware)."""

from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
_RUNTIME_ROOT_SUBDIRS = {"logs", "state"}


def get_data_dir() -> Path:
    """
    Return persistent runtime directory.

    Priority:
    1) DATA_DIR env (recommended, e.g. /data on Railway)
    2) /data when running on Railway and no DATA_DIR is set
    3) local project root for non-Railway runs
    """
    data_dir = (os.getenv("DATA_DIR") or "").strip()
    if data_dir:
        return Path(data_dir)

    if os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_PROJECT_ID"):
        return Path("/data")

    return PROJECT_ROOT


def resolve_runtime_path(path_value: str) -> Path:
    """
    Resolve runtime paths for logs/state to the persistent data directory.

    Relative paths outside logs/state stay project-relative for compatibility.
    """
    path = Path(path_value)
    if path.is_absolute():
        return path

    if path.parts and path.parts[0] in _RUNTIME_ROOT_SUBDIRS:
        return get_data_dir() / path

    return PROJECT_ROOT / path
