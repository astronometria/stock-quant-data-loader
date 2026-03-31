-- ===================================================================
-- Seed data for the first price-domain slice
-- ===================================================================
-- This seed is intentionally tiny.
-- Its purpose is to validate:
-- - instrument-linked price history
-- - symbol-to-instrument price serving
-- - price history and as-of endpoints
-- ===================================================================

INSERT INTO core.price_history (
    price_history_id,
    instrument_id,
    price_date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    source_name,
    observed_at
)
SELECT * FROM (
    VALUES
        (4001, 'INS_AAPL_001', DATE '2019-12-30', 72.365, 73.1725, 71.305, 72.88, 72.88, 144114400, 'seed_v1', TIMESTAMP '2019-12-30 16:00:00'),
        (4002, 'INS_AAPL_001', DATE '2019-12-31', 72.4825, 73.42,   72.38,  73.4125, 73.4125, 100805600, 'seed_v1', TIMESTAMP '2019-12-31 16:00:00'),
        (4003, 'INS_AAPL_001', DATE '2020-01-02', 74.06,   75.15,   73.7975,75.0875, 75.0875, 135480400, 'seed_v1', TIMESTAMP '2020-01-02 16:00:00'),
        (4004, 'INS_AAPL_001', DATE '2020-01-03', 74.2875, 75.145,  74.125, 74.3575, 74.3575, 146322800, 'seed_v1', TIMESTAMP '2020-01-03 16:00:00'),

        (4011, 'INS_MSFT_001', DATE '2019-12-30', 158.986, 159.02,  156.73, 157.59,  157.59,   16348400, 'seed_v1', TIMESTAMP '2019-12-30 16:00:00'),
        (4012, 'INS_MSFT_001', DATE '2019-12-31', 156.77,  157.77,  156.45, 157.70,  157.70,   18369400, 'seed_v1', TIMESTAMP '2019-12-31 16:00:00'),
        (4013, 'INS_MSFT_001', DATE '2020-01-02', 158.78,  160.73,  158.33, 160.62,  160.62,   22622100, 'seed_v1', TIMESTAMP '2020-01-02 16:00:00'),
        (4014, 'INS_MSFT_001', DATE '2020-01-03', 158.32,  159.95,  158.06, 158.62,  158.62,   21116200, 'seed_v1', TIMESTAMP '2020-01-03 16:00:00'),

        (4021, 'INS_SPY_001', DATE '1993-01-29', 43.9687, 44.0000, 43.7500, 43.9375, 43.9375, 1003200, 'seed_v1', TIMESTAMP '1993-01-29 16:00:00'),
        (4022, 'INS_SPY_001', DATE '1993-02-01', 43.9687, 44.2500, 43.9687, 44.2500, 44.2500,  480500, 'seed_v1', TIMESTAMP '1993-02-01 16:00:00'),
        (4023, 'INS_SPY_001', DATE '1993-02-02', 44.2187, 44.3750, 44.1250, 44.3437, 44.3437,  201300, 'seed_v1', TIMESTAMP '1993-02-02 16:00:00'),
        (4024, 'INS_SPY_001', DATE '1993-02-03', 44.4062, 44.8437, 44.3750, 44.8125, 44.8125,  529400, 'seed_v1', TIMESTAMP '1993-02-03 16:00:00')
) AS seed_data(
    price_history_id,
    instrument_id,
    price_date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    source_name,
    observed_at
)
WHERE NOT EXISTS (
    SELECT 1
    FROM core.price_history existing
    WHERE existing.price_history_id = seed_data.price_history_id
);
