"""
Probe the current instrument classification layer to extract symbols that are
still unresolved / UNKNOWN.

Design goals:
- SQL-first
- no guessing
- fully auditable output
- easy to use for building manual JSON overrides

Outputs:
- logs/unknown_instrument_classifications.json
- logs/unknown_instrument_classifications_summary.json
"""

from __future__ import annotations

import json
from pathlib import Path

from stock_quant_data.db.connections import connect_build_db


def run_probe_unknown_instrument_classifications() -> dict:
    """
    Query the resolved current classification view and export the symbols that
    still have UNKNOWN instrument/security classification.

    Returns a small summary dict for console output.
    """
    project_root = Path(__file__).resolve().parents[3]
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    output_rows_path = logs_dir / "unknown_instrument_classifications.json"
    output_summary_path = logs_dir / "unknown_instrument_classifications_summary.json"

    connection = connect_build_db()

    try:
        # Keep the query explicit and fully auditable.
        # We query the resolved current view because this is the layer the user
        # wants to curate manually next.
        rows = connection.execute(
            """
            SELECT
                symbol_normalized,
                source_family,
                source_symbol_native,
                market_code,
                exchange_code,
                listing_venue_bucket,
                instrument_type,
                security_family,
                classification_confidence,
                classification_source,
                classification_rule,
                override_effective_from,
                override_effective_to,
                override_priority,
                override_notes,
                source_file_path,
                source_file_name,
                ingested_at
            FROM normalized.symbol_instrument_classification_current
            WHERE instrument_type = 'UNKNOWN'
               OR security_family = 'UNKNOWN'
            ORDER BY symbol_normalized, source_family, source_symbol_native
            """
        ).fetchall()

        columns = [
            "symbol_normalized",
            "source_family",
            "source_symbol_native",
            "market_code",
            "exchange_code",
            "listing_venue_bucket",
            "instrument_type",
            "security_family",
            "classification_confidence",
            "classification_source",
            "classification_rule",
            "override_effective_from",
            "override_effective_to",
            "override_priority",
            "override_notes",
            "source_file_path",
            "source_file_name",
            "ingested_at",
        ]

        # Build JSON rows with explicit field names for easy manual editing and review.
        row_dicts = [dict(zip(columns, row)) for row in rows]

        output_rows_path.write_text(
            json.dumps(row_dicts, indent=2, default=str),
            encoding="utf-8",
        )

        distinct_symbol_count = connection.execute(
            """
            SELECT COUNT(DISTINCT symbol_normalized)
            FROM normalized.symbol_instrument_classification_current
            WHERE instrument_type = 'UNKNOWN'
               OR security_family = 'UNKNOWN'
            """
        ).fetchone()[0]

        source_breakdown = connection.execute(
            """
            SELECT
                source_family,
                COUNT(*) AS row_count
            FROM normalized.symbol_instrument_classification_current
            WHERE instrument_type = 'UNKNOWN'
               OR security_family = 'UNKNOWN'
            GROUP BY source_family
            ORDER BY source_family
            """
        ).fetchall()

        summary = {
            "output_rows_path": str(output_rows_path),
            "output_summary_path": str(output_summary_path),
            "unknown_row_count": len(row_dicts),
            "unknown_distinct_symbol_count": distinct_symbol_count,
            "source_breakdown": [
                {"source_family": row[0], "row_count": row[1]}
                for row in source_breakdown
            ],
        }

        output_summary_path.write_text(
            json.dumps(summary, indent=2),
            encoding="utf-8",
        )

        return summary
    finally:
        connection.close()


if __name__ == "__main__":
    result = run_probe_unknown_instrument_classifications()
    print(json.dumps(result, indent=2))
