/*
    Semantic Layer: Charger Dimension

    Conformed dimension for charger/station analytics.
    Source: cur_chargers (curated layer)
    Includes data quality flags as dimension attributes for filtering.

    SCD Type 1 - overwrites on change (no history tracking)
*/

{{ config(
    materialized='table',
    tags=['semantic', 'dimension', 'charger']
) }}

WITH chargers AS (
    SELECT * FROM {{ ref('cur_chargers') }}
),

charger_dimension AS (
    SELECT
        -- Surrogate Key (using natural key as surrogate for simplicity)
        charger_id AS charger_key,

        -- Natural Key
        charger_id,

        -- Location Attributes
        city AS charger_city,
        city_original AS charger_city_original,
        latitude AS charger_latitude,
        longitude AS charger_longitude,

        -- Installation Lifecycle
        installed_at AS charger_installed_at,
        DATE(installed_at) AS charger_installed_date,
        TO_NUMBER(TO_CHAR(DATE(installed_at), 'YYYYMMDD')) AS installed_date_key,
        DATEDIFF('day', installed_at, CURRENT_TIMESTAMP()) AS charger_age_days,
        CASE
            WHEN DATEDIFF('day', installed_at, CURRENT_TIMESTAMP()) < 90 THEN 'New (< 90 days)'
            WHEN DATEDIFF('day', installed_at, CURRENT_TIMESTAMP()) < 365 THEN 'Recent (90-365 days)'
            WHEN DATEDIFF('day', installed_at, CURRENT_TIMESTAMP()) < 730 THEN 'Established (1-2 years)'
            ELSE 'Mature (2+ years)'
        END AS charger_age_band,

        -- Data Quality Flags (for filtering in dashboards)
        is_invalid_location,
        is_unmapped_city,
        CASE
            WHEN is_invalid_location = TRUE OR is_unmapped_city = TRUE
            THEN TRUE
            ELSE FALSE
        END AS has_data_quality_issue,

        -- Data Quality Status (for display)
        CASE
            WHEN is_invalid_location = TRUE AND is_unmapped_city = TRUE
                THEN 'Invalid Location & Unmapped City'
            WHEN is_invalid_location = TRUE
                THEN 'Invalid Location'
            WHEN is_unmapped_city = TRUE
                THEN 'Unmapped City'
            ELSE 'Valid'
        END AS data_quality_status,

        -- Metadata
        _loaded_at,
        _source_file,
        _dbt_updated_at,
        CURRENT_TIMESTAMP() AS _semantic_updated_at

    FROM chargers
)

SELECT * FROM charger_dimension
