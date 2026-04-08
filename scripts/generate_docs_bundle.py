#!/usr/bin/env python3
"""
Assemble un bundle documentaire Markdown à partir :
- d'un inventaire repo JSON
- d'un inventaire DB JSON

Le but est d'obtenir une doc finale lisible sans réécrire à la main.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def write_text(path: Path, content: str) -> None:
    """Écrit un fichier texte UTF-8."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_overview(repo_report: dict, db_report: dict) -> str:
    """Construit une vue d'ensemble concise."""
    return f"""# Overview

## Repo

- Python files scanned: **{repo_report.get('python_file_count', 0)}**
- Parsed modules: **{repo_report.get('parsed_module_count', 0)}**
- Failed files: **{len(repo_report.get('failed_files', []))}**

## DB

- Objects inventoried: **{len(db_report.get('objects', []))}**
- DB path: `{db_report.get('db_path', '')}`

## Recommended reading order

1. `01-repo-inventory.md`
2. `02-db-inventory.md`
3. `03-jobs-catalog.md`
4. `04-functions-and-classes.md`
5. `05-db-tables.md`
"""


def build_repo_inventory_md(repo_report: dict) -> str:
    """Construit un markdown synthétique pour les modules."""
    lines = ["# Repo inventory", ""]
    for module in repo_report.get("modules", []):
        lines.append(f"## {module['module']}")
        lines.append("")
        lines.append(f"- path: `{module['path']}`")
        lines.append(f"- functions: **{len(module.get('functions', []))}**")
        lines.append(f"- classes: **{len(module.get('classes', []))}**")
        lines.append("")
    return "\n".join(lines)


def build_jobs_catalog_md(repo_report: dict) -> str:
    """Filtre surtout les modules jobs pour un catalogue opératoire."""
    lines = ["# Jobs catalog", ""]
    modules = repo_report.get("modules", [])
    job_modules = [m for m in modules if ".jobs." in m["module"] or m["module"].endswith(".jobs")]
    for module in job_modules:
        lines.append(f"## {module['module']}")
        lines.append("")
        if module.get("top_level_docstring"):
            lines.append(module["top_level_docstring"])
            lines.append("")
        for fn in module.get("functions", []):
            lines.append(f"- `{fn['signature']}`")
        lines.append("")
    return "\n".join(lines)


def build_functions_classes_md(repo_report: dict) -> str:
    """Construit un index plus détaillé des fonctions/classes."""
    lines = ["# Functions and classes", ""]
    for module in repo_report.get("modules", []):
        if not module.get("functions") and not module.get("classes"):
            continue
        lines.append(f"## {module['module']}")
        lines.append("")
        if module.get("functions"):
            lines.append("### Functions")
            lines.append("")
            for fn in module["functions"]:
                lines.append(f"- `{fn['signature']}`")
            lines.append("")
        if module.get("classes"):
            lines.append("### Classes")
            lines.append("")
            for cls in module["classes"]:
                lines.append(f"- `{cls['name']}`")
                for method in cls.get("methods", []):
                    lines.append(f"  - `{method['signature']}`")
            lines.append("")
    return "\n".join(lines)


def build_db_inventory_md(db_report: dict) -> str:
    """Résumé DB simple."""
    lines = ["# DB inventory", ""]
    for obj in db_report.get("objects", []):
        lines.append(f"## {obj['schema_name']}.{obj['object_name']}")
        lines.append("")
        lines.append(f"- type: `{obj['object_type']}`")
        lines.append(f"- row_count: `{obj['row_count']}`")
        lines.append("")
    return "\n".join(lines)


def build_db_tables_md(db_report: dict) -> str:
    """Version détaillée des tables/colonnes."""
    lines = ["# DB tables", ""]
    for obj in db_report.get("objects", []):
        lines.append(f"## {obj['schema_name']}.{obj['object_name']}")
        lines.append("")
        for col in obj.get("columns", []):
            lines.append(
                f"- `{col['column_name']}` — {col['data_type']} — nullable={col['is_nullable']}"
            )
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Assemble final docs bundle from repo + DB reports.")
    parser.add_argument("--repo-report", required=True, help="Path to repo_inventory.json")
    parser.add_argument("--db-report", required=True, help="Path to db_inventory.json")
    parser.add_argument("--output-dir", required=True, help="Output docs directory")
    args = parser.parse_args()

    repo_report = json.loads(Path(args.repo_report).read_text(encoding="utf-8"))
    db_report = json.loads(Path(args.db_report).read_text(encoding="utf-8"))
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    write_text(output_dir / "00-overview.md", build_overview(repo_report, db_report))
    write_text(output_dir / "01-repo-inventory.md", build_repo_inventory_md(repo_report))
    write_text(output_dir / "02-db-inventory.md", build_db_inventory_md(db_report))
    write_text(output_dir / "03-jobs-catalog.md", build_jobs_catalog_md(repo_report))
    write_text(output_dir / "04-functions-and-classes.md", build_functions_classes_md(repo_report))
    write_text(output_dir / "05-db-tables.md", build_db_tables_md(db_report))

    print(f"Documentation bundle written to: {output_dir}")


if __name__ == "__main__":
    main()
