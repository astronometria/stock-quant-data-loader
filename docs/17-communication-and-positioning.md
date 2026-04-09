# Communication and positioning

## Purpose

This document explains how the repository should present itself to a new reader.

## Current positioning to communicate

The repo should be described as:

- the **loader / build DB** repository of the stock-quant-data stack
- the place where raw datasets are converted into a coherent local DuckDB build database
- the layer that produces identity, listing, and universe structures for downstream consumers

## One-sentence positioning

Suggested short positioning sentence:

```text
Loader DuckDB repo for the stock-quant-data stack: builds identity history, normalized prices, listing status, and universe membership history.
```

## Messaging priorities

When someone lands on the repo, they should understand:

1. what the repo does
2. what the repo does not do
3. the canonical entrypoint
4. where to read next

## What to avoid

Avoid opening with too much internal history or too many documentation versions.

A new reader does not need:

- every past iteration label up front
- multiple equivalent readme-like files at the same level
- generated inventories before they know what the repo is

## Recommended front-page message

The root page should communicate:

- this repo is the loader
- this repo produces the build DB
- use `scripts/rebuild_loader_db.py` for the canonical rebuild
- follow the docs map for the rest

## Recommended audience framing

This repo mainly serves:

- the repo owner / operator
- future collaborators
- downstream stack maintainers
- auditors who need to understand the build DB lineage

## Recommended communication principle

Prefer:

```text
clear role
-> clear entrypoint
-> clear reading path
-> detailed inventories later
```

over:

```text
all technical details immediately
```

## Practical checklist

- short GitHub description
- topics set on GitHub
- short README with a crisp role definition
- one docs map file
- generated docs clearly labeled as generated
