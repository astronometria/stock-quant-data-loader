# 04 — État réel actuel de la DB

## Build DB

```text
/home/marty/stock-quant-data-loader/data/build/market_build.duckdb
```

## Volumes confirmés

- `instrument` = **12677**
- `symbol_reference_history` = **17899**
- `price_source_daily_normalized` = **27397651**
- `listing_status_history` = **17877**
- `universe_membership_history` = **11159**

## Listing status

- `ACTIVE / present_in_latest_complete_nasdaq_snapshot_day` = **12298**
- `ACTIVE / open_symbol_reference_not_confirmed_by_latest_complete_nasdaq_snapshot_day` = **158**
- `INACTIVE / closed_symbol_reference_interval` = **5421**

## Univers

- `US_LISTED_COMMON_STOCKS` = **6257**
- `US_LISTED_ETFS` = **4902**

## Lecture opérationnelle

Le pipeline est dans un état utilisable.  
Le résiduel reviewable restant est assez petit pour ne plus bloquer la suite.
