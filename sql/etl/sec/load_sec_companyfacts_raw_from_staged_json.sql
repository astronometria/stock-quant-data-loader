-- SQL-first loader for SEC companyfacts.
--
-- Design goals:
-- - Let DuckDB parse and explode the JSON structure in SQL.
-- - Avoid Python loops over every observation.
-- - Avoid building giant Python lists in RAM.
-- - Keep one canonical insert path into sec_companyfacts_raw.
--
-- Placeholder:
--   __STAGE_GLOB__
--
-- Expected source:
--   <latest_stage_dir>/*.json
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
WITH
src AS (
    SELECT
        filename,
        regexp_extract(filename, '[^/]+$') AS json_member_name,
        trim(cik) AS cik,
        facts
    FROM read_json_auto(
        '__STAGE_GLOB__',
        columns = {cik: 'VARCHAR', facts: 'JSON'},
        union_by_name = true,
        filename = true
    )
),

-- facts is an object keyed by namespace, for example us-gaap / dei.
fact_namespaces AS (
    SELECT
        filename,
        json_member_name,
        cik,
        ns.key AS fact_namespace,
        ns.value AS namespace_obj
    FROM src,
    json_each(src.facts) AS ns
),

-- Each namespace object is itself keyed by fact name.
fact_definitions AS (
    SELECT
        filename,
        json_member_name,
        cik,
        fact_namespace,
        fd.key AS fact_name,
        fd.value AS fact_obj
    FROM fact_namespaces,
    json_each(fact_namespaces.namespace_obj) AS fd
),

-- The current SEC companyfacts shape stores observations under $.units.<unit_name>.
fact_units AS (
    SELECT
        filename,
        json_member_name,
        cik,
        fact_namespace,
        fact_name,
        unit_entry.key AS unit_name,
        unit_entry.value AS observations_json
    FROM fact_definitions,
    json_each(json_extract(fact_obj, '$.units')) AS unit_entry
),

-- Each observation is an element of the unit array.
fact_observations AS (
    SELECT
        filename,
        json_member_name,
        cik,
        fact_namespace,
        fact_name,
        unit_name,
        obs.value AS obs_json
    FROM fact_units,
    json_each(fact_units.observations_json) AS obs
),

typed AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                cik,
                fact_namespace,
                fact_name,
                unit_name,
                filename,
                json_extract_string(obs_json, '$.accn'),
                json_extract_string(obs_json, '$.filed'),
                json_extract_string(obs_json, '$.end')
        ) AS raw_id,

        cik,
        fact_namespace,
        fact_name,

        CASE
            WHEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE) IS NOT NULL
                THEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE)
            ELSE NULL
        END AS fact_value_double,

        CASE
            WHEN try_cast(json_extract_string(obs_json, '$.val') AS DOUBLE) IS NULL
                THEN json_extract_string(obs_json, '$.val')
            ELSE NULL
        END AS fact_value_text,

        unit_name,
        try_cast(json_extract_string(obs_json, '$.end') AS DATE) AS period_end,
        try_cast(json_extract_string(obs_json, '$.filed') AS DATE) AS filing_date,
        json_extract_string(obs_json, '$.accn') AS accession_number,

        -- We keep the file path in source_zip_path because that is the closest
        -- currently available provenance field in the target schema.
        filename AS source_zip_path,
        json_member_name
    FROM fact_observations
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
FROM typed;
