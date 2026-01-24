/*
    Curated Layer: Chargers

    Standardizes city names using seed mapping table.
    Validates geographic coordinates are within Switzerland bounds.
    Adds data quality flags for downstream filtering.

    Switzerland Bounding Box:
    - Latitude:  45.817995 to 47.808455
    - Longitude: 5.955911 to 10.492294
*/
WITH chargers AS (
    SELECT * FROM {{ ref('stg_chargers') }}
),

city_mapping AS (
    SELECT * FROM {{ ref('city_name_mapping') }}
),

cleaned AS (
    SELECT
        -- Primary Key
        chargers.charger_id,

        -- Standardized city name (fallback to original if no mapping found)
        COALESCE(
            city_mapping.city_name_standardized,
            chargers.city
        ) AS city,

        -- Original city name for audit purposes
        chargers.city AS city_original,

        -- Location coordinates
        chargers.latitude,
        chargers.longitude,

        -- Timestamps
        chargers.installed_at,

        -- Data Quality Flags
        CASE
            WHEN chargers.latitude IS NULL OR chargers.longitude IS NULL
                 OR NOT (
                     chargers.latitude BETWEEN {{ var('switzerland_lat_min') }} AND {{ var('switzerland_lat_max') }}
                     AND chargers.longitude BETWEEN {{ var('switzerland_lon_min') }} AND {{ var('switzerland_lon_max') }}
                 )
            THEN TRUE
            ELSE FALSE
        END AS is_invalid_location,

        CASE
            WHEN city_mapping.city_name_standardized IS NULL
            THEN TRUE
            ELSE FALSE
        END AS is_unmapped_city,

        -- Metadata
        chargers._loaded_at,
        chargers._source_file,
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM chargers
    LEFT JOIN city_mapping
        ON LOWER(TRIM(chargers.city)) = LOWER(TRIM(city_mapping.city_name_raw))
)

SELECT * FROM cleaned
