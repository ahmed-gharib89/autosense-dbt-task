/*
    Stage Layer: Transactions

    Parses raw VARIANT JSON data into typed columns.
    Preserves null end_time for failed transactions.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_transactions') }}
),

parsed AS (
    SELECT
        -- Business Keys
        raw_data:session_id::VARCHAR AS session_id,

        -- Foreign Keys
        raw_data:user_id::VARCHAR AS user_id,
        raw_data:charger_id::VARCHAR AS charger_id,

        -- Transaction Details
        TRY_TO_TIMESTAMP_NTZ(raw_data:start_time::VARCHAR) AS start_time,
        TRY_TO_TIMESTAMP_NTZ(raw_data:end_time::VARCHAR) AS end_time,
        TRY_TO_NUMBER(raw_data:kWh_consumed::VARCHAR, 10, 4) AS kwh_consumed,
        raw_data:payment_method::VARCHAR AS payment_method,
        raw_data:status::VARCHAR AS status,

        -- Metadata
        _loaded_at,
        _source_file,
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM source
)

SELECT * FROM parsed
