/*
    Stage Layer: Chargers

    Parses raw VARIANT JSON data into typed columns.
    Flattens nested location object into separate lat/lon columns.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_chargers') }}
),

parsed AS (
    SELECT
        -- Business Keys
        raw_data:charger_id::VARCHAR AS charger_id,

        -- Charger Attributes
        raw_data:city::VARCHAR AS city,

        -- Location (flattened from nested object)
        raw_data:location:lat::FLOAT AS latitude,
        raw_data:location:lon::FLOAT AS longitude,

        -- Timestamps
        TRY_TO_TIMESTAMP_NTZ(raw_data:installed_at::VARCHAR) AS installed_at,

        -- Metadata
        _loaded_at,
        _source_file,
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM source
)

SELECT * FROM parsed
