/*
    Stage Layer: Users

    Parses raw VARIANT JSON data into typed columns.
    Minimal transformation - preserves original data structure.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_users') }}
),

parsed AS (
    SELECT
        -- Business Keys
        raw_data:user_id::VARCHAR AS user_id,

        -- User Attributes
        raw_data:name::VARCHAR AS name,
        raw_data:email::VARCHAR AS email,
        raw_data:tier::VARCHAR AS tier,

        -- Timestamps
        TRY_TO_TIMESTAMP_NTZ(raw_data:created_at::VARCHAR) AS created_at,

        -- Metadata
        _loaded_at,
        _source_file,
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM source
)

SELECT * FROM parsed
