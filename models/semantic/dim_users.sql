/*
    Semantic Layer: User Dimension

    Conformed dimension for user analytics.
    Source: cur_users (curated layer)

    SCD Type 1 - overwrites on change (no history tracking)
*/

{{ config(
    materialized='table',
    tags=['semantic', 'dimension', 'user']
) }}

WITH users AS (
    SELECT * FROM {{ ref('cur_users') }}
),

user_dimension AS (
    SELECT
        -- Surrogate Key (using natural key as surrogate for simplicity)
        user_id AS user_key,

        -- Natural Key
        user_id,

        -- User Attributes
        name AS user_name,
        email AS user_email,

        -- Tier Analysis
        tier AS user_tier,
        CASE
            WHEN tier = 'subscriber' THEN 1
            WHEN tier = 'guest' THEN 2
            ELSE 99
        END AS user_tier_sort_order,
        CASE
            WHEN tier = 'subscriber' THEN 'Premium'
            WHEN tier = 'guest' THEN 'Basic'
            ELSE 'Unknown'
        END AS user_tier_display,

        -- User Lifecycle
        created_at AS user_created_at,
        DATE(created_at) AS user_created_date,
        DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) AS user_tenure_days,
        CASE
            WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) < 30 THEN 'New (< 30 days)'
            WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) < 90 THEN 'Recent (30-90 days)'
            WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) < 365 THEN 'Established (90-365 days)'
            ELSE 'Veteran (1+ year)'
        END AS user_tenure_band,

        -- Metadata
        _loaded_at,
        _source_file,
        _dbt_updated_at,
        CURRENT_TIMESTAMP() AS _semantic_updated_at

    FROM users
)

SELECT * FROM user_dimension
