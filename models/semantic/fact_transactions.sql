/*
    Semantic Layer: Transactions Fact Table

    Central fact table for EV charging analytics.
    Grain: One row per charging session (transaction).

    Joins transactions with payments for complete financial picture.
    Includes dimension keys for star schema joins.
    Supports all dashboard KPIs and analysis requirements.
*/

{{ config(
    materialized='table',
    tags=['semantic', 'fact', 'transaction']
) }}

WITH transactions AS (
    SELECT * FROM {{ ref('cur_transactions') }}
),

payments AS (
    SELECT * FROM {{ ref('cur_payments') }}
),

chargers AS (
    SELECT charger_id, city FROM {{ ref('cur_chargers') }}
),

fact_transactions AS (
    SELECT
        -- Primary Key
        transactions.session_id,

        -- Dimension Keys (for star schema joins)
        transactions.user_id AS user_key,
        transactions.charger_id AS charger_key,
        chargers.city AS city_name,  -- Denormalized for easier joins

        -- Date Keys (for date dimension joins)
        TO_NUMBER(TO_CHAR(DATE(transactions.start_time), 'YYYYMMDD')) AS transaction_date_key,
        DATE(transactions.start_time) AS transaction_date,

        -- Transaction Timestamps
        transactions.start_time,
        transactions.end_time,

        -- Measures: Energy
        transactions.kwh_consumed AS energy_kwh,
        COALESCE(transactions.kwh_consumed, 0) AS energy_kwh_coalesced,

        -- Measures: Duration
        transactions.charging_duration_minutes AS duration_minutes,
        COALESCE(transactions.charging_duration_minutes, 0) AS duration_minutes_coalesced,

        -- Measures: Financial (from payments)
        payments.amount AS payment_amount,
        COALESCE(payments.amount, 0) AS payment_amount_coalesced,
        payments.currency AS payment_currency,

        -- Derived Measures
        CASE
            WHEN transactions.kwh_consumed > 0 AND payments.amount IS NOT NULL
            THEN payments.amount / transactions.kwh_consumed
            ELSE NULL
        END AS revenue_per_kwh,

        CASE
            WHEN transactions.charging_duration_minutes > 0 AND transactions.kwh_consumed IS NOT NULL
            THEN transactions.kwh_consumed / (transactions.charging_duration_minutes / 60.0)
            ELSE NULL
        END AS charging_rate_kw,  -- Average kW during session

        -- Transaction Attributes
        transactions.payment_method,
        transactions.status AS transaction_status,

        -- Payment Attributes
        payments.payment_id,

        -- Data Quality Flags (from transactions)
        transactions.is_invalid_time_range,
        transactions.is_missing_kwh,
        transactions.is_negative_kwh,
        transactions.is_outlier_kwh,
        transactions.is_outlier_duration,

        -- Data Quality Flags (from payments)
        CASE WHEN payments.payment_id IS NULL THEN TRUE ELSE FALSE END AS is_missing_payment,
        COALESCE(payments.is_missing_amount, FALSE) AS is_missing_payment_amount,
        COALESCE(payments.is_invalid_amount, FALSE) AS is_invalid_payment_amount,
        COALESCE(payments.is_outlier_amount, FALSE) AS is_outlier_payment_amount,

        -- Composite Data Quality Flag
        CASE
            WHEN transactions.is_invalid_time_range = TRUE
                OR transactions.is_missing_kwh = TRUE
                OR transactions.is_negative_kwh = TRUE
                OR transactions.is_outlier_kwh = TRUE
                OR transactions.is_outlier_duration = TRUE
                OR payments.payment_id IS NULL
                OR COALESCE(payments.is_missing_amount, FALSE) = TRUE
                OR COALESCE(payments.is_invalid_amount, FALSE) = TRUE
                OR COALESCE(payments.is_outlier_amount, FALSE) = TRUE
            THEN TRUE
            ELSE FALSE
        END AS has_any_quality_issue,

        -- Valid Transaction Flag (for clean analysis)
        CASE
            WHEN transactions.status = 'completed'
                AND transactions.is_invalid_time_range = FALSE
                AND transactions.is_missing_kwh = FALSE
                AND transactions.is_negative_kwh = FALSE
                AND payments.payment_id IS NOT NULL
                AND COALESCE(payments.is_invalid_amount, FALSE) = FALSE
            THEN TRUE
            ELSE FALSE
        END AS is_valid_transaction,

        -- Counting Measures (for aggregations)
        1 AS transaction_count,
        CASE WHEN transactions.status = 'completed' THEN 1 ELSE 0 END AS completed_transaction_count,
        CASE WHEN transactions.status = 'failed' THEN 1 ELSE 0 END AS failed_transaction_count,

        -- Metadata
        transactions._loaded_at,
        transactions._source_file,
        transactions._dbt_updated_at AS _curated_updated_at,
        CURRENT_TIMESTAMP() AS _semantic_updated_at

    FROM transactions
    LEFT JOIN payments
        ON transactions.session_id = payments.session_id
    LEFT JOIN chargers
        ON transactions.charger_id = chargers.charger_id
)

SELECT * FROM fact_transactions
