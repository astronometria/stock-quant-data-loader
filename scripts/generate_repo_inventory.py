#!/usr/bin/env python3
"""
Génère un inventaire technique automatique du repo stock-quant-data-loader.

Ce script :
- parcourt le repo Python
- cible surtout src/stock_quant_data/jobs
- extrait les modules, imports, fonctions, classes, signatures
- produit un JSON et un Markdown de synthèse

Le but est de fournir une base documentaire fiable, régénérable,
et surtout beaucoup moins dépendante de suppositions manuelles.
"""

from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from tqdm import tqdm


@dataclass
class FunctionInfo:
    """Représente une fonction Python extraite depuis l'AST."""
    name: str
    signature: str
    lineno: int
    end_lineno: int | None
    decorators: list[str]
    docstring: str | None


@dataclass
class ClassInfo:
    """Représente une classe Python extraite depuis l'AST."""
    name: str
    lineno: int
    end_lineno: int | None
    bases: list[str]
    decorators: list[str]
    docstring: str | None
    methods: list[FunctionInfo]


@dataclass
class ModuleInfo:
    """Représente un module Python scanné."""
    path: str
    module: str
    imports: list[str]
    functions: list[FunctionInfo]
    classes: list[ClassInfo]
    top_level_docstring: str | None


def safe_unparse(node: ast.AST | None) -> str:
    """
    Convertit un noeud AST en texte.
    On reste tolérant si ast.unparse échoue.
    """
    if node is None:
        return ""
    try:
        return ast.unparse(node)
    except Exception:
        return "<unparse_failed>"


def format_function_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """
    Reconstruit une signature lisible depuis un noeud fonction.
    Ce n'est pas une signature runtime parfaite, mais une représentation
    suffisamment utile pour la documentation et l'audit.
    """
    args = []

    # Arguments positionnels simples
    positional = list(node.args.args)
    defaults = list(node.args.defaults)
    default_offset = len(positional) - len(defaults)

    for idx, arg in enumerate(positional):
        arg_text = arg.arg
        if arg.annotation:
            arg_text += f": {safe_unparse(arg.annotation)}"
        if idx >= default_offset:
            default_value = defaults[idx - default_offset]
            arg_text += f" = {safe_unparse(default_value)}"
        args.append(arg_text)

    # *args
    if node.args.vararg:
        vararg = "*" + node.args.vararg.arg
        if node.args.vararg.annotation:
            vararg += f": {safe_unparse(node.args.vararg.annotation)}"
        args.append(vararg)

    # keyword-only
    for kwarg, kwdefault in zip(node.args.kwonlyargs, node.args.kw_defaults):
        arg_text = kwarg.arg
        if kwarg.annotation:
            arg_text += f": {safe_unparse(kwarg.annotation)}"
        if kwdefault is not None:
            arg_text += f" = {safe_unparse(kwdefault)}"
        args.append(arg_text)

    # **kwargs
    if node.args.kwarg:
        kwarg = "**" + node.args.kwarg.arg
        if node.args.kwarg.annotation:
            kwarg += f": {safe_unparse(node.args.kwarg.annotation)}"
        args.append(kwarg)

    signature = f"{node.name}({', '.join(args)})"
    if node.returns:
        signature += f" -> {safe_unparse(node.returns)}"
    return signature


def extract_imports(tree: ast.AST) -> list[str]:
    """Extrait tous les imports top-level d'un module."""
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imported = ", ".join(alias.name for alias in node.names)
            imports.append(f"from {module} import {imported}")
    return sorted(set(imports))


def extract_functions(nodes: list[ast.stmt]) -> list[FunctionInfo]:
    """Extrait uniquement les fonctions top-level passées en entrée."""
    result: list[FunctionInfo] = []
    for node in nodes:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            result.append(
                FunctionInfo(
                    name=node.name,
                    signature=format_function_signature(node),
                    lineno=node.lineno,
                    end_lineno=getattr(node, "end_lineno", None),
                    decorators=[safe_unparse(d) for d in node.decorator_list],
                    docstring=ast.get_docstring(node),
                )
            )
    return result


def extract_classes(nodes: list[ast.stmt]) -> list[ClassInfo]:
    """Extrait les classes top-level et leurs méthodes."""
    classes: list[ClassInfo] = []
    for node in nodes:
        if isinstance(node, ast.ClassDef):
            methods = []
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    methods.append(
                        FunctionInfo(
                            name=child.name,
                            signature=format_function_signature(child),
                            lineno=child.lineno,
                            end_lineno=getattr(child, "end_lineno", None),
                            decorators=[safe_unparse(d) for d in child.decorator_list],
                            docstring=ast.get_docstring(child),
                        )
                    )
            classes.append(
                ClassInfo(
                    name=node.name,
                    lineno=node.lineno,
                    end_lineno=getattr(node, "end_lineno", None),
                    bases=[safe_unparse(b) for b in node.bases],
                    decorators=[safe_unparse(d) for d in node.decorator_list],
                    docstring=ast.get_docstring(node),
                    methods=methods,
                )
            )
    return classes


def file_to_module(repo_root: Path, path: Path) -> str:
    """Convertit un chemin de fichier en nom de module Python."""
    relative = path.relative_to(repo_root)
    parts = list(relative.with_suffix("").parts)
    return ".".join(parts)


def scan_python_file(repo_root: Path, path: Path) -> ModuleInfo | None:
    """
    Parse un fichier Python et renvoie une structure riche.
    On retourne None si le parse AST échoue.
    """
    source = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None

    top_level_nodes = list(tree.body)
    return ModuleInfo(
        path=str(path.relative_to(repo_root)),
        module=file_to_module(repo_root, path),
        imports=extract_imports(tree),
        functions=extract_functions(top_level_nodes),
        classes=extract_classes(top_level_nodes),
        top_level_docstring=ast.get_docstring(tree),
    )


def render_markdown(modules: list[dict[str, Any]]) -> str:
    """Construit un rapport Markdown lisible."""
    lines: list[str] = []
    lines.append("# Repo inventory")
    lines.append("")
    lines.append(f"Modules scannés: **{len(modules)}**")
    lines.append("")

    for module in modules:
        lines.append(f"## {module['module']}")
        lines.append("")
        lines.append(f"- path: `{module['path']}`")
        lines.append(f"- imports: **{len(module['imports'])}**")
        lines.append(f"- functions: **{len(module['functions'])}**")
        lines.append(f"- classes: **{len(module['classes'])}**")
        lines.append("")

        if module["functions"]:
            lines.append("### Functions")
            lines.append("")
            for fn in module["functions"]:
                lines.append(f"- `{fn['signature']}`")
            lines.append("")

        if module["classes"]:
            lines.append("### Classes")
            lines.append("")
            for cls in module["classes"]:
                lines.append(f"- `{cls['name']}`")
                for method in cls["methods"]:
                    lines.append(f"  - `{method['signature']}`")
            lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate repo inventory from Python source files.")
    parser.add_argument("--repo-root", required=True, help="Path to repository root.")
    parser.add_argument("--output-dir", required=True, help="Directory where reports are written.")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    python_files = sorted(
        p for p in repo_root.rglob("*.py")
        if ".venv" not in p.parts and "__pycache__" not in p.parts
    )

    scanned_modules: list[dict[str, Any]] = []
    failed_files: list[str] = []

    for path in tqdm(python_files, desc="Scanning python files", unit="file"):
        info = scan_python_file(repo_root, path)
        if info is None:
            failed_files.append(str(path.relative_to(repo_root)))
            continue
        scanned_modules.append(asdict(info))

    report = {
        "repo_root": str(repo_root),
        "python_file_count": len(python_files),
        "parsed_module_count": len(scanned_modules),
        "failed_files": failed_files,
        "modules": scanned_modules,
    }

    json_path = output_dir / "repo_inventory.json"
    md_path = output_dir / "repo_inventory.md"

    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_markdown(scanned_modules), encoding="utf-8")

    print(f"JSON report written to: {json_path}")
    print(f"Markdown report written to: {md_path}")
    print(f"Failed files: {len(failed_files)}")


if __name__ == "__main__":
    main()
