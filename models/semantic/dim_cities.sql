/*
    Semantic Layer: City Dimension

    Conformed dimension for geographic/city-level analytics.
    Combines city_name_mapping seed with charger locations.
    Supports the Geographic Performance dashboard requirement.
*/

{{ config(
    materialized='table',
    tags=['semantic', 'dimension', 'city', 'geography']
) }}

WITH city_mapping AS (
    SELECT
        city_name_standardized AS city_name,
        LISTAGG(DISTINCT city_name_raw, ', ') WITHIN GROUP (ORDER BY city_name_raw) AS city_name_variants
    FROM {{ ref('city_name_mapping') }}
    GROUP BY city_name_standardized
),

charger_cities AS (
    SELECT
        city AS city_name,
        COUNT(*) AS charger_count,
        AVG(latitude) AS avg_latitude,
        AVG(longitude) AS avg_longitude,
        MIN(installed_at) AS first_charger_installed_at,
        MAX(installed_at) AS last_charger_installed_at,
        SUM(CASE WHEN is_invalid_location = FALSE THEN 1 ELSE 0 END) AS valid_location_chargers,
        SUM(CASE WHEN is_invalid_location = TRUE THEN 1 ELSE 0 END) AS invalid_location_chargers
    FROM {{ ref('cur_chargers') }}
    GROUP BY city
),

city_dimension AS (
    SELECT
        -- Surrogate Key (hash-based for consistency)
        {{ dbt_utils.generate_surrogate_key(['charger_cities.city_name']) }} AS city_key,

        -- Natural Key
        charger_cities.city_name,

        -- City Variants (for search/display)
        COALESCE(city_mapping.city_name_variants, charger_cities.city_name) AS city_name_variants,

        -- Geographic Center (average of charger locations)
        charger_cities.avg_latitude AS city_center_latitude,
        charger_cities.avg_longitude AS city_center_longitude,

        -- Charger Statistics
        charger_cities.charger_count AS total_chargers,
        charger_cities.valid_location_chargers,
        charger_cities.invalid_location_chargers,

        -- Timeline
        charger_cities.first_charger_installed_at,
        charger_cities.last_charger_installed_at,
        DATEDIFF('day', charger_cities.first_charger_installed_at, charger_cities.last_charger_installed_at) AS expansion_period_days,

        -- City Size Classification (based on charger count)
        CASE
            WHEN charger_cities.charger_count >= 10 THEN 'Large Market'
            WHEN charger_cities.charger_count >= 5 THEN 'Medium Market'
            WHEN charger_cities.charger_count >= 2 THEN 'Small Market'
            ELSE 'Emerging Market'
        END AS market_size_tier,

        -- Data Quality Indicator
        CASE
            WHEN city_mapping.city_name IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS is_unmapped_city,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM charger_cities
    LEFT JOIN city_mapping
        ON charger_cities.city_name = city_mapping.city_name
)

SELECT * FROM city_dimension
