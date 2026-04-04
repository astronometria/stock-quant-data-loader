"""
Stage SEC companyfacts JSON members from downloader artifacts into a local
filesystem staging directory.

Why this job exists:
- it decouples raw zip reading from DB insertion
- it keeps Python thin: unzip + write staged json
- later loader jobs can read staged files deterministically

Current canonical downstream target:
- load_sec_companyfacts_raw_from_staged_json.py
"""

from __future__ import annotations

import json
import logging
import zipfile
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings

LOGGER = logging.getLogger(__name__)


def _latest_zip_path(root: Path) -> Path:
    """
    Return the latest companyfacts zip path.
    """
    candidates = sorted(root.glob("*.zip"))
    if not candidates:
        raise FileNotFoundError(f"No companyfacts zip found under {root}")
    return candidates[-1]


def run() -> None:
    """
    Extract companyfacts json members into the local staging directory.
    """
    configure_logging()
    LOGGER.info("stage-sec-companyfacts-json-from-downloader started")

    settings = get_settings()

    downloader_root = Path(settings.data_root).parent / "stock-quant-data-downloader" / "data" / "sec" / "companyfacts"
    if not downloader_root.exists():
        downloader_root = Path(settings.data_root) / "sec" / "companyfacts"

    latest_zip = _latest_zip_path(downloader_root)

    stage_dir = Path(settings.data_root) / "sec" / "companyfacts_staged"
    stage_dir.mkdir(parents=True, exist_ok=True)

    # Clean current staged json files for deterministic rebuild behavior.
    for path in stage_dir.glob("*.json"):
        path.unlink()

    staged_count = 0

    with zipfile.ZipFile(latest_zip, "r") as zf:
        members = sorted([name for name in zf.namelist() if name.lower().endswith(".json")])

        for member_name in tqdm(members, desc="sec_companyfacts_stage", unit="json"):
            payload = json.loads(zf.read(member_name).decode("utf-8"))

            # Keep a simple flat filename that remains stable and filesystem-safe.
            cik = str(payload.get("cik", "")).strip() or "UNKNOWN"
            out_path = stage_dir / f"{cik}_{Path(member_name).name}"

            with out_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, separators=(",", ":"))

            staged_count += 1

    print(
        {
            "status": "ok",
            "job": "stage-sec-companyfacts-json-from-downloader",
            "latest_zip": str(latest_zip),
            "stage_dir": str(stage_dir),
            "staged_count": staged_count,
        }
    )

    LOGGER.info("stage-sec-companyfacts-json-from-downloader finished")


if __name__ == "__main__":
    run()
