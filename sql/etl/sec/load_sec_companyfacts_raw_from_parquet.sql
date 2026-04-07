-- ============================================================================
-- SQL-first loader for SEC companyfacts from derived Parquet
-- ============================================================================
-- Design goals:
-- - Keep the ingestion path SQL-first.
-- - Reuse the already-built Parquet layer instead of reparsing JSON.
-- - Keep Python thin: resolve the latest parquet batch, then execute this SQL.
-- - Keep the target table contract identical to the current raw loader.
--
-- Placeholder expected to be replaced by Python before execution:
--   __PARQUET_GLOB__
--
-- Expected source parquet columns:
--   cik
--   fact_namespace
--   fact_name
--   fact_value_double
--   fact_value_text
--   unit_name
--   period_end
--   filing_date
--   accession_number
--   source_file_path
--   json_member_name
--
-- Expected target schema:
--   sec_companyfacts_raw(
--       raw_id,
--       cik,
--       fact_namespace,
--       fact_name,
--       fact_value_double,
--       fact_value_text,
--       unit_name,
--       period_end,
--       filing_date,
--       accession_number,
--       source_zip_path,
--       json_member_name
--   )
-- ============================================================================

DELETE FROM sec_companyfacts_raw;

INSERT INTO sec_companyfacts_raw (
    raw_id,
    cik,
    fact_namespace,
    fact_name,
    fact_value_double,
    fact_value_text,
    unit_name,
    period_end,
    filing_date,
    accession_number,
    source_zip_path,
    json_member_name
)
WITH ordered AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                cik,
                fact_namespace,
                fact_name,
                unit_name,
                source_file_path,
                accession_number,
                filing_date,
                period_end
        ) AS raw_id,
        trim(cik) AS cik,
        fact_namespace,
        fact_name,
        fact_value_double,
        fact_value_text,
        unit_name,
        period_end,
        filing_date,
        accession_number,
        -- We map the parquet provenance path into source_zip_path because the
        -- existing raw target schema already uses that field name.
        source_file_path AS source_zip_path,
        json_member_name
    FROM read_parquet('__PARQUET_GLOB__')
)
SELECT
    raw_id,
    cik,
    fact_namespace,
    fact_name,
    fact_value_double,
    fact_value_text,
    unit_name,
    period_end,
    filing_date,
    accession_number,
    source_zip_path,
    json_member_name
FROM ordered;
