#!/usr/bin/env python3
"""
Génère un inventaire automatique de la base DuckDB.

Le script :
- liste les tables et vues
- extrait les colonnes
- calcule les counts par table
- produit un JSON et un Markdown de synthèse

Très utile pour figer l’état réel de la base au moment de l’audit.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
from tqdm import tqdm


def fetch_objects(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]:
    """
    Retourne les objets relationnels principaux:
    (schema_name, object_name, object_type)
    """
    return conn.execute(
        """
        SELECT
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
        """
    ).fetchall()


def fetch_columns(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[dict]:
    """Récupère les colonnes d'une table / vue."""
    rows = conn.execute(
        """
        SELECT
            column_name,
            data_type,
            is_nullable,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return [
        {
            "column_name": r[0],
            "data_type": r[1],
            "is_nullable": r[2],
            "ordinal_position": r[3],
        }
        for r in rows
    ]


def safe_count(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> int | None:
    """
    Tente un COUNT(*). Certaines vues peuvent échouer;
    on retourne alors None.
    """
    try:
        return conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
    except Exception:
        return None


def render_markdown(report: dict) -> str:
    """Construit une synthèse Markdown."""
    lines: list[str] = []
    lines.append("# DB inventory")
    lines.append("")
    lines.append(f"DB path: `{report['db_path']}`")
    lines.append(f"Object count: **{len(report['objects'])}**")
    lines.append("")

    for obj in report["objects"]:
        lines.append(f"## {obj['schema_name']}.{obj['object_name']}")
        lines.append("")
        lines.append(f"- type: `{obj['object_type']}`")
        lines.append(f"- row_count: `{obj['row_count']}`")
        lines.append("")
        lines.append("### Columns")
        lines.append("")
        for col in obj["columns"]:
            lines.append(
                f"- `{col['column_name']}` — {col['data_type']} — nullable={col['is_nullable']}"
            )
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate DuckDB schema inventory.")
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database.")
    parser.add_argument("--output-dir", required=True, help="Directory where reports are written.")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    try:
        objects = fetch_objects(conn)

        object_reports = []
        for schema_name, object_name, object_type in tqdm(objects, desc="Scanning DB objects", unit="obj"):
            object_reports.append(
                {
                    "schema_name": schema_name,
                    "object_name": object_name,
                    "object_type": object_type,
                    "row_count": safe_count(conn, schema_name, object_name),
                    "columns": fetch_columns(conn, schema_name, object_name),
                }
            )

        report = {
            "db_path": str(db_path),
            "objects": object_reports,
        }

        json_path = output_dir / "db_inventory.json"
        md_path = output_dir / "db_inventory.md"

        json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        md_path.write_text(render_markdown(report), encoding="utf-8")

        print(f"JSON report written to: {json_path}")
        print(f"Markdown report written to: {md_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
