# stock-quant-data-loader

## What this repo is

`stock-quant-data-loader` is the **loader / build-database** repository of the stock-quant-data stack.

Its role is to:

- initialize the local DuckDB build database
- load and normalize raw market / identity datasets
- maintain identity history
- build listing status history
- build research-ready universe membership history

## What this repo is not

This repo is **not** the final serving API layer.

It is the **loader-side build system** that produces the canonical build DB used by downstream layers.

## Canonical entrypoint

For a full rebuild, use:

```bash
python3 scripts/rebuild_loader_db.py
```

## Read the docs in this order

1. `docs/00-doc-map.md`
2. `docs/11-runbook-canonique.md`
3. `docs/12-runtime-paths-and-env.md`
4. `docs/13-identity-status-universe-model.md`
5. `docs/14-db-current-state.md`
6. `docs/15-jobs-by-sequence.md`
7. `docs/16-troubleshooting.md`

## Important repo areas

- `src/stock_quant_data/jobs/` — unit jobs
- `src/stock_quant_data/cli/` — CLI layer
- `scripts/` — orchestration and support scripts
- `data/build/market_build.duckdb` — local build DB
- `docs/` — curated documentation
- `docs/auto_generated/` — generated inventory

## Communication goal

This repository should be understood as:

```text
raw / staged data
    ->
identity + normalization
    ->
listing status
    ->
universe membership
    ->
downstream consumers
```
