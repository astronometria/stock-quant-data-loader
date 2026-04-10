-- Canonical provider-symbol mapping history for fetchable market data providers.
CREATE TABLE IF NOT EXISTS instrument_provider_symbol_history (
    instrument_provider_symbol_history_id BIGINT PRIMARY KEY,
    instrument_id BIGINT NOT NULL,
    provider_name VARCHAR NOT NULL,
    provider_symbol VARCHAR NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    symbol_role VARCHAR NOT NULL,
    mapping_status VARCHAR NOT NULL,
    confidence_score DOUBLE,
    source_name VARCHAR,
    source_priority INTEGER,
    source_detail VARCHAR,
    discovered_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ipsh_provider_symbol
ON instrument_provider_symbol_history(provider_name, provider_symbol);

CREATE INDEX IF NOT EXISTS idx_ipsh_instrument_provider
ON instrument_provider_symbol_history(instrument_id, provider_name);

CREATE INDEX IF NOT EXISTS idx_ipsh_active_window
ON instrument_provider_symbol_history(provider_name, mapping_status, effective_from, effective_to);

CREATE OR REPLACE VIEW v_instrument_provider_symbol_active AS
SELECT
    instrument_id,
    provider_name,
    provider_symbol,
    effective_from,
    effective_to,
    symbol_role,
    mapping_status,
    confidence_score,
    source_name,
    source_priority,
    source_detail
FROM instrument_provider_symbol_history
WHERE mapping_status = 'ACTIVE'
  AND symbol_role = 'PRIMARY_FETCH_SYMBOL';
