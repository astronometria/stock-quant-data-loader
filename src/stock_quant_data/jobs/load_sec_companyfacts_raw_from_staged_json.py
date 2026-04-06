"""
SQL-first SEC companyfacts loader, batched by file groups.

Why this version exists:
- One giant read_json_auto('*.json') query over ~20k files can stall inside one
  opaque SQL execution step.
- This version keeps the transformation SQL-first, but executes it in batches of
  file paths so progress is visible and resource usage is bounded.

Design:
- Python is intentionally thin orchestration only.
- DuckDB still performs the JSON reading and explosion work in SQL.
- We append into sec_companyfacts_raw batch by batch.
"""

from __future__ import annotations

import logging
from pathlib import Path

from tqdm import tqdm

from stock_quant_data.config.logging import configure_logging
from stock_quant_data.config.settings import get_settings
from stock_quant_data.db.connections import connect_build_db

LOGGER = logging.getLogger(__name__)

# Tune this if needed.
# Smaller batch => more round trips, but faster visible progress and lower risk
# of giant opaque SQL stalls.
FILE_BATCH_SIZE = 200


def _latest_stage_dir(stage_root: Path) -> Path:
    candidates = sorted([p for p in stage_root.iterdir() if p.is_dir()])
    if not candidates:
        raise FileNotFoundError(f"No staged companyfacts batch found under {stage_root}")
    return candidates[-1]


def _chunked(items: list[Path], chunk_size: int) -> list[list[Path]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_batch_sql(batch_files: list[Path]) -> str:
    """
    Build one SQL-first insert for a specific file batch.

    We use read_json_auto over an explicit list of file paths instead of a giant
    wildcard across the whole batch directory.
    """
    file_list_sql = ", ".join(_quote_sql_string(str(p)) for p in batch_files)

    return f"""
    INSERT INTO sec_companyfacts_raw (
        raw_id,
        cik,
        fact_namespace,
        fact_name,
        fact_value_double,
        fact_value_text,
        unit_name,
        period_end,
        filing_date,
        accession_number,
        source_zip_path,
        json_member_name
    )
    WITH
    current_max AS (
        SELECT COALESCE(MAX(raw_id), 0) AS max_raw_id
        FROM sec_companyfacts_raw
    ),
    src AS (
        SELECT
            filename,
            regexp_extract(filename, '[^/]+$') AS json_member_name,
            trim(cik) AS cik,
            facts
        FROM read_json_auto(
            [{file_list_sql}],
            columns = {{cik: 'VARCHAR', facts: 'JSON'}},
            union_by_name = true,
            filename = true
        )
    ),
    fact_namespaces AS (
        SELECT
            filename,
            json_member_name,
            cik,
            ns.key AS fact_namespace,
            ns.value AS namespace_obj
        FROM src,
        json_each(src.facts) AS ns
    ),
    fact_definitions AS (
        SELECT
            filename,
            json_member_name,
            cik,
            fact_namespace,
            fd.key AS fact_name,
            fd.value AS fact_obj
        FROM fact_namespaces,
        json_each(fact_namespaces.namespace_obj) AS fd
    ),
    fact_units AS (
        SELECT
            filename,
            json_member_name,
            cik,
            fact_namespace,
            fact_name,
            unit_entry.key AS unit_name,
            unit_entry.value AS observations_json
        FROM fact_definitions,
        json_each(json_extract(fact_obj, '$.units')) AS unit_entry
    ),
    fact_observations AS (
        SELECT
            filename,
            json_member_name,
            cik,
            fact_namespace,
            fact_name,
            unit_name,
            obs.value AS obs_json
        FROM fact_units,
        json_each(fact_units.observations_json) AS obs
    ),
    typed AS (
        SELECT
            (SELECT max_raw_id FROM current_max)
            + ROW_NUMBER() OVER (
                ORDER BY
                    cik,
                    fact_namespace,
                    fact_name,
                    unit_name,
                    filename,
                    json_extract_string(obs_json, '$.accn'),
                    json_extract_string(obs_json, '$.filed'),
                    json_extract_string(obs_json, '$.end')
            ) AS raw_id,

            cik,
            fact_namespace,
            fact_name,

            CASE
                WHEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE) IS NOT NULL
                    THEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE)
                ELSE NULL
            END AS fact_value_double,

            CASE
                WHEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE) IS NULL
                    THEN json_extract_string(obs_json, '$.val')
                ELSE NULL
            END AS fact_value_text,

            unit_name,
            try_cast(json_extract_string(obs_json, '$.end') AS DATE) AS period_end,
            try_cast(json_extract_string(obs_json, '$.filed') AS DATE) AS filing_date,
            json_extract_string(obs_json, '$.accn') AS accession_number,
            filename AS source_zip_path,
            json_member_name
        FROM fact_observations
    )
    SELECT
        raw_id,
        cik,
        fact_namespace,
        fact_name,
        fact_value_double,
        fact_value_text,
        unit_name,
        period_end,
        filing_date,
        accession_number,
        source_zip_path,
        json_member_name
    FROM typed
    """


def run() -> None:
    configure_logging()
    LOGGER.info("load-sec-companyfacts-raw-from-staged-json started")

    settings = get_settings()
    stage_root = Path(settings.data_root) / "staging" / "sec" / "companyfacts"
    if not stage_root.exists():
        raise FileNotFoundError(f"Staging root not found: {stage_root}")

    stage_dir = _latest_stage_dir(stage_root)
    json_files = sorted(stage_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No staged companyfacts json files found in {stage_dir}")

    file_batches = _chunked(json_files, FILE_BATCH_SIZE)

    conn = connect_build_db()
    try:
        conn.execute("DELETE FROM sec_companyfacts_raw")

        batch_bar = tqdm(file_batches, desc="sec_companyfacts_batches", unit="batch")
        for batch_index, batch_files in enumerate(batch_bar, start=1):
            sql_text = _build_batch_sql(batch_files)
            conn.execute(sql_text)

            current_count = conn.execute(
                "SELECT COUNT(*) FROM sec_companyfacts_raw"
            ).fetchone()[0]

            batch_bar.set_postfix(
                batch=batch_index,
                total_batches=len(file_batches),
                files_in_batch=len(batch_files),
                rows_loaded=current_count,
            )

        final_row_count = conn.execute(
            "SELECT COUNT(*) FROM sec_companyfacts_raw"
        ).fetchone()[0]

        print(
            {
                "status": "ok",
                "job": "load-sec-companyfacts-raw-from-staged-json",
                "stage_dir": str(stage_dir),
                "json_file_count": len(json_files),
                "file_batch_size": FILE_BATCH_SIZE,
                "batch_count": len(file_batches),
                "final_row_count": final_row_count,
                "mode": "sql_first_batched",
            }
        )
    finally:
        conn.close()
        LOGGER.info("load-sec-companyfacts-raw-from-staged-json finished")


if __name__ == "__main__":
    run()
