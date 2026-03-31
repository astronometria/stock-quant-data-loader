-- ===================================================================
-- Foundation scientific validation checks
-- ===================================================================
-- These checks are designed to be publication blockers.
--
-- A release should not be published if foundational historical tables
-- violate basic temporal integrity.
--
-- Conventions:
-- - Each query returns exactly one row
-- - "check_name" identifies the check
-- - "failed_rows" is the number of violating rows
-- - Publication passes only if all failed_rows = 0
-- ===================================================================

SELECT
    'symbol_reference_history_invalid_window' AS check_name,
    COUNT(*) AS failed_rows
FROM core.symbol_reference_history
WHERE effective_to IS NOT NULL
  AND effective_to < effective_from

UNION ALL

SELECT
    'listing_status_history_invalid_window' AS check_name,
    COUNT(*) AS failed_rows
FROM core.listing_status_history
WHERE effective_to IS NOT NULL
  AND effective_to < effective_from

UNION ALL

SELECT
    'universe_membership_history_invalid_window' AS check_name,
    COUNT(*) AS failed_rows
FROM core.universe_membership_history
WHERE effective_to IS NOT NULL
  AND effective_to < effective_from

UNION ALL

SELECT
    'symbol_reference_history_overlapping_windows' AS check_name,
    COUNT(*) AS failed_rows
FROM (
    SELECT
        a.symbol_reference_history_id AS left_id,
        b.symbol_reference_history_id AS right_id
    FROM core.symbol_reference_history a
    JOIN core.symbol_reference_history b
      ON a.instrument_id = b.instrument_id
     AND a.symbol = b.symbol
     AND a.symbol_reference_history_id < b.symbol_reference_history_id
     AND a.effective_from < COALESCE(b.effective_to, DATE '2999-12-31')
     AND b.effective_from < COALESCE(a.effective_to, DATE '2999-12-31')
) AS overlap_rows_1

UNION ALL

SELECT
    'listing_status_history_overlapping_windows' AS check_name,
    COUNT(*) AS failed_rows
FROM (
    SELECT
        a.listing_status_history_id AS left_id,
        b.listing_status_history_id AS right_id
    FROM core.listing_status_history a
    JOIN core.listing_status_history b
      ON a.instrument_id = b.instrument_id
     AND a.symbol = b.symbol
     AND a.listing_status_history_id < b.listing_status_history_id
     AND a.effective_from < COALESCE(b.effective_to, DATE '2999-12-31')
     AND b.effective_from < COALESCE(a.effective_to, DATE '2999-12-31')
) AS overlap_rows_2

UNION ALL

SELECT
    'universe_membership_history_overlapping_windows' AS check_name,
    COUNT(*) AS failed_rows
FROM (
    SELECT
        a.universe_membership_history_id AS left_id,
        b.universe_membership_history_id AS right_id
    FROM core.universe_membership_history a
    JOIN core.universe_membership_history b
      ON a.universe_id = b.universe_id
     AND a.instrument_id = b.instrument_id
     AND a.universe_membership_history_id < b.universe_membership_history_id
     AND a.effective_from < COALESCE(b.effective_to, DATE '2999-12-31')
     AND b.effective_from < COALESCE(a.effective_to, DATE '2999-12-31')
) AS overlap_rows_3
;
