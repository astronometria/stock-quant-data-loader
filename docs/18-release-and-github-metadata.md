# Release and GitHub metadata

## Purpose

This document lists the GitHub-level metadata that should be improved to make the repository easier to understand from the outside.

## GitHub description

Suggested repository description:

```text
Loader DuckDB repo for the stock-quant-data stack: builds identity history, normalized prices, listing status, and universe membership history.
```

## Suggested topics

Suggested GitHub topics:

- `duckdb`
- `python`
- `etl`
- `data-loader`
- `quant`
- `market-data`
- `sec-filings`
- `universe-history`

## Releases

The repo would benefit from at least one baseline release.

Suggested initial tag idea:

```text
v0.1-doc-baseline
```

Purpose of the first release:

- mark the first coherent documentation baseline
- make the repo feel less like a private working tree
- give future diffs a stable reference point

## Recommended release note content

A baseline release note could state:

- curated docs added
- generated inventory published
- loader role clarified
- canonical rebuild entrypoint documented

## Why this matters

Without description, topics, or releases, a technically strong repo can still look unfinished.

Metadata communicates:

- maturity
- scope
- discoverability
- intent

## Minimal external-facing checklist

- set repository description
- add GitHub topics
- publish one baseline release
- keep `README.md` aligned with current repo scope
- keep docs map linked from README

## Communication result expected

Once metadata is improved, a new reader should be able to understand the repo in under a minute:

1. repo description
2. README opening
3. docs map
4. canonical rebuild command
