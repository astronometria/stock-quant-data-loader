"""
Stage SEC companyfacts JSON files from downloader ZIP archives onto local disk.

Why this job exists:
- The downloader repo stores SEC companyfacts as ZIP archives.
- We want the platform repo to work SQL-first from staged JSON files.
- This job keeps Python very thin: it only performs archive discovery and extraction.

Design:
- Extract every companyfacts ZIP found under the downloader root.
- Each ZIP is extracted into its own snapshot directory:
    data/staging/sec/companyfacts/<snapshot_id>/
- The snapshot_id is derived from the ZIP filename stem with the "_companyfacts" suffix removed.
- Existing staged snapshot directories are replaced so reruns stay deterministic.

Important:
- This is a staging step only.
- It does not parse the JSON payloads.
- It does not write to DuckDB.
"""

from __future__ import annotations

import logging
import shutil
import zipfile
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging

LOGGER = logging.getLogger(__name__)

DOWNLOADER_COMPANYFACTS_ROOT = Path(
    "/home/marty/stock-quant-data-downloader/data/sec/companyfacts"
)

STAGING_ROOT = Path(
    "/home/marty/stock-quant-data-platform/data/staging/sec/companyfacts"
)


def snapshot_id_from_zip_path(zip_path: Path) -> str:
    """
    Derive a stable snapshot id from the ZIP filename.

    Example:
    2026-03-29T03-18-32-857281Z_46bd7ae3_companyfacts.zip
    ->
    2026-03-29T03-18-32-857281Z_46bd7ae3
    """
    name = zip_path.name
    suffix = "_companyfacts.zip"
    if name.endswith(suffix):
        return name[: -len(suffix)]
    return zip_path.stem


def run() -> None:
    """
    Extract companyfacts ZIP archives into versioned staging folders.
    """
    configure_logging()
    LOGGER.info("stage-sec-companyfacts-json-from-downloader started")

    if not DOWNLOADER_COMPANYFACTS_ROOT.exists():
        raise FileNotFoundError(
            f"Downloader companyfacts root does not exist: {DOWNLOADER_COMPANYFACTS_ROOT}"
        )

    STAGING_ROOT.mkdir(parents=True, exist_ok=True)

    zip_paths = sorted(
        path
        for path in DOWNLOADER_COMPANYFACTS_ROOT.glob("*.zip")
        if path.is_file()
    )

    extracted_snapshot_count = 0
    extracted_json_count = 0

    for zip_path in tqdm(
        zip_paths,
        desc="sec_companyfacts_zip",
        unit="zip",
        dynamic_ncols=True,
        leave=True,
    ):
        snapshot_id = snapshot_id_from_zip_path(zip_path)
        snapshot_stage_dir = STAGING_ROOT / snapshot_id

        # Rebuild the staged snapshot directory each run for deterministic behavior.
        if snapshot_stage_dir.exists():
            shutil.rmtree(snapshot_stage_dir)
        snapshot_stage_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zf:
            members = sorted(
                name
                for name in zf.namelist()
                if name.lower().endswith(".json")
            )

            for member_name in tqdm(
                members,
                desc=f"extract:{snapshot_id}",
                unit="json",
                dynamic_ncols=True,
                leave=False,
            ):
                zf.extract(member_name, path=snapshot_stage_dir)
                extracted_json_count += 1

        extracted_snapshot_count += 1

    print(
        {
            "status": "ok",
            "job": "stage-sec-companyfacts-json-from-downloader",
            "downloader_root": str(DOWNLOADER_COMPANYFACTS_ROOT),
            "staging_root": str(STAGING_ROOT),
            "zip_count": len(zip_paths),
            "snapshot_count": extracted_snapshot_count,
            "json_count": extracted_json_count,
        }
    )

    LOGGER.info("stage-sec-companyfacts-json-from-downloader finished")


if __name__ == "__main__":
    run()
