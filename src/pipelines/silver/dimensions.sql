-- ============================================================
-- SILVER DIMENSIONS & REFERENCE DATA
-- Clinical Trial Intelligence Platform
-- ============================================================
-- Purpose:
-- 1. Clean and standardize Bronze master/reference datasets
-- 2. Deduplicate using defined business keys
-- 3. Establish explicit Silver data types
-- 4. Preserve source lineage metadata
-- 5. Enrich site geography using governed reference mappings
-- ============================================================


-- ============================================================
-- 1. DIMENSION: INSTITUTION
-- Source: bronze.master_institutions
-- Business Key: institution_id
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_institution
COMMENT 'Cleaned and standardized institution dimension derived from Bronze master institutions'
AS

WITH cleaned AS (
    SELECT
        TRIM(institution_id)   AS institution_id,
        TRIM(institution_name) AS institution_name,
        TRIM(city)             AS city,
        TRIM(country)          AS country,
        TRIM(region)           AS region,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(institution_id)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.master_institutions

    WHERE institution_id IS NOT NULL
      AND TRIM(institution_id) <> ''
)

SELECT
    institution_id,
    institution_name,
    city,
    country,
    region,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 2. DIMENSION: SPONSOR
-- Source: bronze.master_sponsors
-- Business Key: sponsor_id
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_sponsor
COMMENT 'Cleaned and standardized sponsor dimension derived from Bronze master sponsors'
AS

WITH cleaned AS (
    SELECT
        TRIM(sponsor_id)   AS sponsor_id,
        TRIM(sponsor_name) AS sponsor_name,
        TRIM(sponsor_type) AS sponsor_type,
        TRIM(hq_country)   AS hq_country,
        TRIM(hq_city)      AS hq_city,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(sponsor_id)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.master_sponsors

    WHERE sponsor_id IS NOT NULL
      AND TRIM(sponsor_id) <> ''
)

SELECT
    sponsor_id,
    sponsor_name,
    sponsor_type,
    hq_country,
    hq_city,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 3. DIMENSION: PRODUCT
-- Source: bronze.master_products
-- Business Key: product_id
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_product
COMMENT 'Cleaned and standardized product dimension derived from Bronze master products'
AS

WITH cleaned AS (
    SELECT
        TRIM(product_id)          AS product_id,
        TRIM(sponsor_id)          AS sponsor_id,
        TRIM(product_code)        AS product_code,
        TRIM(product_name)        AS product_name,
        TRIM(active_ingredient)   AS active_ingredient,
        TRIM(modality)            AS modality,
        TRIM(mechanism_of_action) AS mechanism_of_action,
        TRIM(product_status)      AS product_status,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(product_id)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.master_products

    WHERE product_id IS NOT NULL
      AND TRIM(product_id) <> ''
)

SELECT
    product_id,
    sponsor_id,
    product_code,
    product_name,
    active_ingredient,
    modality,
    mechanism_of_action,
    product_status,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 4. REFERENCE: COUNTRY / REGION
-- Source: bronze.ref_country_region
-- Business Key: country
-- ============================================================
-- Created before its logical consumer for readability.
-- Lakeflow constructs dependencies from dataset references.
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW ref_country_region
COMMENT 'Standardized country to region reference mapping'
AS

WITH cleaned AS (
    SELECT
        TRIM(country) AS country,
        TRIM(region)  AS region,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(country))
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ref_country_region

    WHERE country IS NOT NULL
      AND TRIM(country) <> ''
)

SELECT
    country,
    region,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 5. REFERENCE: GEOGRAPHY
-- Source: bronze.ref_geography
-- Business Key: city + country
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW ref_geography
COMMENT 'Standardized city, country and region geography reference mapping'
AS

WITH cleaned AS (
    SELECT
        TRIM(city)    AS city,
        TRIM(country) AS country,
        TRIM(region)  AS region,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY
                LOWER(TRIM(city)),
                LOWER(TRIM(country))
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ref_geography

    WHERE city IS NOT NULL
      AND TRIM(city) <> ''
      AND country IS NOT NULL
      AND TRIM(country) <> ''
)

SELECT
    city,
    country,
    region,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 6. DIMENSION: SITE
-- Source: bronze.ctms_sites
-- References:
--   ref_geography       -> city + country mapping
--   ref_country_region  -> country-level fallback
-- Business Key: site_id
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_site
COMMENT 'Cleaned and standardized clinical trial site dimension enriched with governed geographic region'
AS

WITH cleaned AS (
    SELECT
        TRIM(site_id)                AS site_id,
        TRIM(study_id)               AS study_id,
        TRIM(institution_id)         AS institution_id,
        TRIM(site_name)              AS site_name,
        TRIM(country)                AS country,
        TRIM(city)                   AS city,
        TRIM(principal_investigator) AS principal_investigator,

        TRY_CAST(target_subjects AS INT)  AS target_subjects,
        TRY_CAST(activation_date AS DATE) AS activation_date,

        UPPER(TRIM(site_status)) AS site_status,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(site_id)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ctms_sites

    WHERE site_id IS NOT NULL
      AND TRIM(site_id) <> ''
),

deduplicated AS (
    SELECT
        site_id,
        study_id,
        institution_id,
        site_name,
        country,
        city,
        principal_investigator,
        target_subjects,
        activation_date,
        site_status,
        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date

    FROM cleaned
    WHERE rn = 1
)

SELECT
    s.site_id,
    s.study_id,
    s.institution_id,
    s.site_name,
    s.country,
    s.city,

    COALESCE(
        g.region,
        cr.region,
        'UNMAPPED'
    ) AS region,

    s.principal_investigator,
    s.target_subjects,
    s.activation_date,
    s.site_status,

    s._source_file,
    s._source_file_name,
    s._source_file_modification_ts,
    s._ingestion_ts,
    s._ingestion_date

FROM deduplicated s

LEFT JOIN ref_geography g
    ON LOWER(TRIM(s.city)) = LOWER(TRIM(g.city))
   AND LOWER(TRIM(s.country)) = LOWER(TRIM(g.country))

LEFT JOIN ref_country_region cr
    ON LOWER(TRIM(s.country)) = LOWER(TRIM(cr.country));



-- ============================================================
-- 7. DIMENSION: STUDY
-- Source: bronze.ctms_studies
-- Business Key: study_id
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_study
COMMENT 'Cleaned and standardized clinical study dimension derived from Bronze CTMS studies'
AS

WITH cleaned AS (
    SELECT
        TRIM(study_id)         AS study_id,
        TRIM(study_name)       AS study_name,
        TRIM(protocol_number)  AS protocol_number,
        TRIM(therapeutic_area) AS therapeutic_area,
        TRIM(phase)            AS phase,
        TRIM(indication_code)  AS indication_code,
        TRIM(sponsor_id)       AS sponsor_id,

        TRY_CAST(target_enrollment AS INT)
            AS target_enrollment,

        TRY_CAST(planned_start_date AS DATE)
            AS planned_start_date,

        TRY_CAST(planned_end_date AS DATE)
            AS planned_end_date,

        UPPER(TRIM(study_status))
            AS study_status,

        TRIM(protocol_version)
            AS protocol_version,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(study_id)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ctms_studies

    WHERE study_id IS NOT NULL
      AND TRIM(study_id) <> ''
)

SELECT
    study_id,
    study_name,
    protocol_number,
    therapeutic_area,
    phase,
    indication_code,
    sponsor_id,
    target_enrollment,
    planned_start_date,
    planned_end_date,
    study_status,
    protocol_version,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 8. DIMENSION: STUDY ARM
-- Source: bronze.protocol_study_arms
-- Business Key: study_id + arm_code
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_study_arm
COMMENT 'Cleaned and standardized clinical study arm dimension derived from Bronze protocol study arms'
AS

WITH cleaned AS (
    SELECT
        TRIM(study_id)     AS study_id,
        TRIM(arm_code)     AS arm_code,
        TRIM(arm_label)    AS arm_label,
        TRIM(product_id)   AS product_id,
        TRIM(product_role) AS product_role,

        TRY_CAST(dose AS INT) AS dose,

        TRIM(dose_unit)       AS dose_unit,
        TRIM(dosing_schedule) AS dosing_schedule,

        TRY_CAST(planned_allocation_pct AS INT)
            AS planned_allocation_pct,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY
                TRIM(study_id),
                TRIM(arm_code)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.protocol_study_arms

    WHERE study_id IS NOT NULL
      AND TRIM(study_id) <> ''
      AND arm_code IS NOT NULL
      AND TRIM(arm_code) <> ''
)

SELECT
    study_id,
    arm_code,
    arm_label,
    product_id,
    product_role,
    dose,
    dose_unit,
    dosing_schedule,
    planned_allocation_pct,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 9. REFERENCE: DIAGNOSIS
-- Source: bronze.ref_diagnosis_mapping
-- Business Key: diagnosis_code
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW ref_diagnosis
COMMENT 'Standardized diagnosis reference mapping'
AS

WITH cleaned AS (
    SELECT
        TRIM(diagnosis_code)        AS diagnosis_code,
        TRIM(diagnosis_description) AS diagnosis_description,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(diagnosis_code)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ref_diagnosis_mapping

    WHERE diagnosis_code IS NOT NULL
      AND TRIM(diagnosis_code) <> ''
)

SELECT
    diagnosis_code,
    diagnosis_description,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 10. REFERENCE: LAB TEST
-- Source: bronze.ref_lab_test
-- Business Key: lab_test_code
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW ref_lab_test
COMMENT 'Standardized laboratory test reference data'
AS

WITH cleaned AS (
    SELECT
        TRIM(lab_test_code) AS lab_test_code,
        TRIM(lab_test_name) AS lab_test_name,
        TRIM(standard_unit) AS standard_unit,

        TRY_CAST(reference_low AS DOUBLE)
            AS reference_low,

        TRY_CAST(reference_high AS DOUBLE)
            AS reference_high,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(lab_test_code)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ref_lab_test

    WHERE lab_test_code IS NOT NULL
      AND TRIM(lab_test_code) <> ''
)

SELECT
    lab_test_code,
    lab_test_name,
    standard_unit,
    reference_low,
    reference_high,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 11. REFERENCE: SEVERITY
-- Source: bronze.ref_severity_mapping
-- Business Key: raw_severity
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW ref_severity
COMMENT 'Standardized clinical severity reference mapping'
AS

WITH cleaned AS (
    SELECT
        TRIM(raw_severity)      AS raw_severity,
        TRIM(standard_severity) AS standard_severity,

        TRY_CAST(severity_rank AS INT)
            AS severity_rank,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(raw_severity)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ref_severity_mapping

    WHERE raw_severity IS NOT NULL
      AND TRIM(raw_severity) <> ''
)

SELECT
    raw_severity,
    standard_severity,
    severity_rank,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 12. REFERENCE: SEX
-- Source: bronze.ref_sex_mapping
-- Business Key: raw_sex
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW ref_sex
COMMENT 'Standardized sex reference mapping'
AS

WITH cleaned AS (
    SELECT
        TRIM(raw_sex)      AS raw_sex,
        TRIM(standard_sex) AS standard_sex,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY TRIM(raw_sex)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ref_sex_mapping

    WHERE raw_sex IS NOT NULL
      AND TRIM(raw_sex) <> ''
)

SELECT
    raw_sex,
    standard_sex,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;



-- ============================================================
-- 13. REFERENCE: UNIT MAPPING
-- Source: bronze.ref_unit_mapping
-- Business Key: lab_test_code + raw_unit
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW ref_unit
COMMENT 'Standardized laboratory unit conversion reference mapping'
AS

WITH cleaned AS (
    SELECT
        TRIM(lab_test_code) AS lab_test_code,
        TRIM(raw_unit)      AS raw_unit,
        TRIM(standard_unit) AS standard_unit,

        TRY_CAST(conversion_factor AS DOUBLE)
            AS conversion_factor,

        _source_file,
        _source_file_name,
        _source_file_modification_ts,
        _ingestion_ts,
        _ingestion_date,

        ROW_NUMBER() OVER (
            PARTITION BY
                TRIM(lab_test_code),
                TRIM(raw_unit)
            ORDER BY _ingestion_ts DESC
        ) AS rn

    FROM clinical_trial_intelligence.bronze.ref_unit_mapping

    WHERE lab_test_code IS NOT NULL
      AND TRIM(lab_test_code) <> ''
      AND raw_unit IS NOT NULL
      AND TRIM(raw_unit) <> ''
)

SELECT
    lab_test_code,
    raw_unit,
    standard_unit,
    conversion_factor,
    _source_file,
    _source_file_name,
    _source_file_modification_ts,
    _ingestion_ts,
    _ingestion_date

FROM cleaned
WHERE rn = 1;