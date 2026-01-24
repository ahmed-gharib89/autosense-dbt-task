/*
    Curated Layer: Users

    User dimension with no data cleansing required.
    Pass-through from staging with consistent metadata.
*/

WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
)

SELECT
    -- Primary Key
    user_id,

    -- User Attributes
    name,
    email,
    tier,

    -- Timestamps
    created_at,

    -- Metadata
    _loaded_at,
    _source_file,
    CURRENT_TIMESTAMP() AS _dbt_updated_at

FROM users
