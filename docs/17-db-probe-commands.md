# Commandes de probe DB

## Objectif

Ce document centralise les probes DuckDB les plus utiles pour auditer rapidement l'état courant de la build DB.

## Base cible

```text
data/build/market_build.duckdb
```

## Probe minimal de santé

```bash
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
for table_name in [
    "instrument",
    "symbol_reference_history",
    "price_source_daily_normalized",
    "listing_status_history",
    "universe_membership_history",
]:
    try:
        print(table_name, conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    except Exception as e:
        print(table_name, "ERROR", e)
conn.close()
PY2
```

## Listing status

```bash
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
for row in conn.execute("""
    SELECT listing_status, status_reason, COUNT(*)
    FROM listing_status_history
    GROUP BY 1, 2
    ORDER BY 1, 2
""").fetchall():
    print(row)
conn.close()
PY2
```

## Univers

```bash
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
for row in conn.execute("""
    SELECT universe_name, COUNT(*)
    FROM universe_membership_history
    GROUP BY 1
    ORDER BY 1
""").fetchall():
    print(row)
conn.close()
PY2
```

## Prix normalisés

```bash
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
print(conn.execute("""
    SELECT COUNT(*), MIN(price_date), MAX(price_date)
    FROM price_source_daily_normalized
""").fetchone())
conn.close()
PY2
```

## Résidus listing non confirmés

```bash
python3 <<'PY2'
import duckdb
conn = duckdb.connect("data/build/market_build.duckdb")
for row in conn.execute("""
    SELECT symbol, effective_from
    FROM listing_status_history
    WHERE listing_status = 'ACTIVE'
      AND status_reason = 'open_symbol_reference_not_confirmed_by_latest_complete_nasdaq_snapshot_day'
    ORDER BY effective_from DESC, symbol
    LIMIT 100
""").fetchall():
    print(row)
conn.close()
PY2
```
