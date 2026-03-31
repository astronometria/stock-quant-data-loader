"""
Publication helpers.

Option B requires a strict separation between:
- build artifacts
- published serving releases

In v1, publication creates a release directory and writes metadata files.
The actual full DB copy / serving projection can be expanded in the next step.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import os

from stock_quant_data.config.settings import get_settings


def utc_release_id() -> str:
    """
    Generate a sortable UTC release identifier.

    Example:
        2026-03-28T18-30-00Z
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def create_release_dir(release_id: str | None = None) -> Path:
    """
    Create a new release directory under the releases root.

    Returns the created release directory path.
    """
    settings = get_settings()
    rid = release_id or utc_release_id()
    release_dir = settings.releases_root / rid
    release_dir.mkdir(parents=True, exist_ok=False)
    return release_dir


def write_manifest(release_dir: Path, payload: dict) -> Path:
    """
    Write the release manifest as pretty JSON.

    The manifest is part of the scientific contract of the release.
    """
    manifest_path = release_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return manifest_path


def switch_current_release_symlink(release_dir: Path) -> None:
    """
    Atomically point the 'current' symlink to the supplied release directory.

    We use os.replace on a temporary symlink to make the switch safer.
    """
    settings = get_settings()
    link_path = settings.current_release_link
    link_path.parent.mkdir(parents=True, exist_ok=True)

    temp_link = link_path.with_name(link_path.name + ".tmp")

    if temp_link.exists() or temp_link.is_symlink():
        temp_link.unlink()

    target = Path(os.path.relpath(release_dir, start=link_path.parent))
    temp_link.symlink_to(target, target_is_directory=True)
    os.replace(temp_link, link_path)
