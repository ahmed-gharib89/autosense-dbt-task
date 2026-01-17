/*
    Stage Layer: Payments

    Parses raw VARIANT JSON data into typed columns.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_payments') }}
),

parsed AS (
    SELECT
        -- Business Keys
        raw_data:payment_id::VARCHAR AS payment_id,

        -- Foreign Keys
        raw_data:session_id::VARCHAR AS session_id,
        raw_data:user_id::VARCHAR AS user_id,

        -- Payment Details
        TRY_TO_NUMBER(raw_data:amount::VARCHAR, 10, 2) AS amount,
        raw_data:currency::VARCHAR AS currency,

        -- Metadata
        _loaded_at,
        _source_file,
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM source
)

SELECT * FROM parsed
