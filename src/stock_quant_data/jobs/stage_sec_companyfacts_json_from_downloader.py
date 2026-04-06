"""
Stage SEC companyfacts JSON from downloader zip into local staging directory.

Canonical staging destination:
- data/staging/sec/companyfacts/<zip_stem>/*.json
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings

LOGGER = logging.getLogger(__name__)


def _latest_companyfacts_zip(downloader_root: Path) -> Path:
    candidates = sorted(downloader_root.glob("*.zip"))
    if not candidates:
        raise FileNotFoundError(f"No companyfacts zip found under {downloader_root}")
    return candidates[-1]


def run() -> None:
    configure_logging()
    LOGGER.info("stage-sec-companyfacts-json-from-downloader started")

    settings = get_settings()

    downloader_root = Path(settings.downloader_data_dir) / "sec" / "companyfacts"
    if not downloader_root.exists():
        downloader_root = Path(settings.data_root) / "sec" / "companyfacts"

    latest_zip = _latest_companyfacts_zip(downloader_root)

    stage_root = Path(settings.data_root) / "staging" / "sec" / "companyfacts" / latest_zip.stem
    stage_root.mkdir(parents=True, exist_ok=True)

    extracted = 0
    with zipfile.ZipFile(latest_zip, "r") as zf:
        members = sorted(name for name in zf.namelist() if name.lower().endswith(".json"))
        for member_name in tqdm(members, desc="stage_companyfacts", unit="json"):
            target_path = stage_root / Path(member_name).name
            target_path.write_bytes(zf.read(member_name))
            extracted += 1

    print(
        {
            "status": "ok",
            "job": "stage-sec-companyfacts-json-from-downloader",
            "latest_zip": str(latest_zip),
            "stage_root": str(stage_root),
            "extracted_json_count": extracted,
        }
    )

    LOGGER.info("stage-sec-companyfacts-json-from-downloader finished")


if __name__ == "__main__":
    run()
