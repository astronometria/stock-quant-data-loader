-- ===================================================================
-- Seed data for the first symbol-domain slice
-- ===================================================================
-- This seed is intentionally tiny.
-- Its purpose is to validate:
-- - stable instrument identity
-- - historical symbol mapping
-- - API serving for symbol history
--
-- We use deterministic IDs so repeated runs stay idempotent.
-- ===================================================================

INSERT INTO core.instrument (
    instrument_id,
    company_id,
    security_type,
    primary_ticker,
    primary_exchange,
    instrument_status
)
SELECT * FROM (
    VALUES
        ('INS_AAPL_001', 'COM_AAPL_001', 'COMMON_STOCK', 'AAPL', 'NASDAQ', 'ACTIVE'),
        ('INS_MSFT_001', 'COM_MSFT_001', 'COMMON_STOCK', 'MSFT', 'NASDAQ', 'ACTIVE'),
        ('INS_SPY_001', 'COM_SPY_001', 'ETF', 'SPY', 'NYSE_ARCA', 'ACTIVE')
) AS seed_data(
    instrument_id,
    company_id,
    security_type,
    primary_ticker,
    primary_exchange,
    instrument_status
)
WHERE NOT EXISTS (
    SELECT 1
    FROM core.instrument existing
    WHERE existing.instrument_id = seed_data.instrument_id
);

INSERT INTO core.symbol_reference_history (
    symbol_reference_history_id,
    instrument_id,
    symbol,
    exchange,
    mic,
    is_primary,
    effective_from,
    effective_to,
    observed_at,
    source_name,
    record_status
)
SELECT * FROM (
    VALUES
        (
            1001,
            'INS_AAPL_001',
            'AAPL',
            'NASDAQ',
            'XNAS',
            TRUE,
            DATE '1980-12-12',
            NULL,
            TIMESTAMP '1980-12-12 00:00:00',
            'seed_v1',
            'ACTIVE'
        ),
        (
            1002,
            'INS_MSFT_001',
            'MSFT',
            'NASDAQ',
            'XNAS',
            TRUE,
            DATE '1986-03-13',
            NULL,
            TIMESTAMP '1986-03-13 00:00:00',
            'seed_v1',
            'ACTIVE'
        ),
        (
            1003,
            'INS_SPY_001',
            'SPY',
            'NYSE_ARCA',
            'ARCX',
            TRUE,
            DATE '1993-01-29',
            NULL,
            TIMESTAMP '1993-01-29 00:00:00',
            'seed_v1',
            'ACTIVE'
        )
) AS seed_data(
    symbol_reference_history_id,
    instrument_id,
    symbol,
    exchange,
    mic,
    is_primary,
    effective_from,
    effective_to,
    observed_at,
    source_name,
    record_status
)
WHERE NOT EXISTS (
    SELECT 1
    FROM core.symbol_reference_history existing
    WHERE existing.symbol_reference_history_id = seed_data.symbol_reference_history_id
);

INSERT INTO core.listing_status_history (
    listing_status_history_id,
    instrument_id,
    symbol,
    exchange,
    listing_status,
    event_type,
    effective_from,
    effective_to,
    observed_at,
    source_name
)
SELECT * FROM (
    VALUES
        (
            2001,
            'INS_AAPL_001',
            'AAPL',
            'NASDAQ',
            'ACTIVE',
            'LISTED',
            DATE '1980-12-12',
            NULL,
            TIMESTAMP '1980-12-12 00:00:00',
            'seed_v1'
        ),
        (
            2002,
            'INS_MSFT_001',
            'MSFT',
            'NASDAQ',
            'ACTIVE',
            'LISTED',
            DATE '1986-03-13',
            NULL,
            TIMESTAMP '1986-03-13 00:00:00',
            'seed_v1'
        ),
        (
            2003,
            'INS_SPY_001',
            'SPY',
            'NYSE_ARCA',
            'ACTIVE',
            'LISTED',
            DATE '1993-01-29',
            NULL,
            TIMESTAMP '1993-01-29 00:00:00',
            'seed_v1'
        )
) AS seed_data(
    listing_status_history_id,
    instrument_id,
    symbol,
    exchange,
    listing_status,
    event_type,
    effective_from,
    effective_to,
    observed_at,
    source_name
)
WHERE NOT EXISTS (
    SELECT 1
    FROM core.listing_status_history existing
    WHERE existing.listing_status_history_id = seed_data.listing_status_history_id
);

INSERT INTO core.universe_membership_history (
    universe_membership_history_id,
    universe_id,
    instrument_id,
    membership_status,
    effective_from,
    effective_to,
    observed_at,
    source_name
)
SELECT * FROM (
    VALUES
        (
            3001,
            'UNIV_US_LISTED_COMMON_STOCKS',
            'INS_AAPL_001',
            'ACTIVE',
            DATE '1980-12-12',
            NULL,
            TIMESTAMP '1980-12-12 00:00:00',
            'seed_v1'
        ),
        (
            3002,
            'UNIV_NASDAQ_LISTED',
            'INS_AAPL_001',
            'ACTIVE',
            DATE '1980-12-12',
            NULL,
            TIMESTAMP '1980-12-12 00:00:00',
            'seed_v1'
        ),
        (
            3003,
            'UNIV_US_LISTED_COMMON_STOCKS',
            'INS_MSFT_001',
            'ACTIVE',
            DATE '1986-03-13',
            NULL,
            TIMESTAMP '1986-03-13 00:00:00',
            'seed_v1'
        ),
        (
            3004,
            'UNIV_NASDAQ_LISTED',
            'INS_MSFT_001',
            'ACTIVE',
            DATE '1986-03-13',
            NULL,
            TIMESTAMP '1986-03-13 00:00:00',
            'seed_v1'
        ),
        (
            3005,
            'UNIV_US_LISTED_ETFS',
            'INS_SPY_001',
            'ACTIVE',
            DATE '1993-01-29',
            NULL,
            TIMESTAMP '1993-01-29 00:00:00',
            'seed_v1'
        )
) AS seed_data(
    universe_membership_history_id,
    universe_id,
    instrument_id,
    membership_status,
    effective_from,
    effective_to,
    observed_at,
    source_name
)
WHERE NOT EXISTS (
    SELECT 1
    FROM core.universe_membership_history existing
    WHERE existing.universe_membership_history_id = seed_data.universe_membership_history_id
);
