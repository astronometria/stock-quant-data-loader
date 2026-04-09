# Documentation map

## Purpose

This file is the **single navigation page** for the repository documentation.

Use it when you arrive in the repo and want to know where to read first.

## Primary reading order

### 1. Operational entrypoint

- `README.md`
- `docs/11-runbook-canonique.md`

Read these first if you want to understand the repo quickly and execute the pipeline in the right order.

### 2. Runtime / environment

- `docs/12-runtime-paths-and-env.md`

Read this when you want to confirm the expected paths, DB location, and environment assumptions.

### 3. Core data model

- `docs/13-identity-status-universe-model.md`

Read this when you want to understand the conceptual pipeline:

```text
identity -> listing -> universe
```

### 4. Current DB state

- `docs/14-db-current-state.md`

Read this when you want a real snapshot-oriented description of the current build DB.

### 5. Job sequencing

- `docs/15-jobs-by-sequence.md`

Read this when you want to understand the loader as an ordered sequence of job families.

### 6. Troubleshooting

- `docs/16-troubleshooting.md`

Read this when something breaks or when output counts look suspicious.

## Generated docs

The folder `docs/auto_generated/` contains generated inventories.

These documents are useful as **technical reference**, but they are not the best starting point for a human reader.

Recommended usage:

- curated docs in `docs/` = human-first
- auto-generated docs = machine-produced inventory / lookup layer

## Recommended communication hierarchy

### Canonical entrypoints

- `README.md`
- `docs/00-doc-map.md`

### Operational docs

- `docs/11-runbook-canonique.md`
- `docs/12-runtime-paths-and-env.md`
- `docs/15-jobs-by-sequence.md`
- `docs/16-troubleshooting.md`

### Conceptual docs

- `docs/13-identity-status-universe-model.md`
- `docs/14-db-current-state.md`

### Generated reference

- `docs/auto_generated/`

## Rule of thumb

If you are new to the repo:

1. read `README.md`
2. read `docs/00-doc-map.md`
3. read the runbook
4. only then dive into generated inventories
