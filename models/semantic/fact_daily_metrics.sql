/*
    Semantic Layer: Daily Metrics Fact Table

    Pre-aggregated daily metrics for dashboard performance.
    Grain: One row per day per city.

    Supports dashboard requirements:
    - KPIs with month-over-month comparison
    - Revenue trend analysis (last 90 days)
    - Data Quality Monitor
*/

{{ config(
    materialized='table',
    tags=['semantic', 'fact', 'metrics', 'aggregated']
) }}

WITH fact_transactions AS (
    SELECT * FROM {{ ref('fact_transactions') }}
),

daily_city_metrics AS (
    SELECT
        -- Grain Keys
        transaction_date,
        city_name,

        -- Date Key for dimension join
        transaction_date_key,

        -- Revenue Metrics
        SUM(payment_amount_coalesced) AS total_revenue,
        SUM(CASE WHEN is_valid_transaction = TRUE THEN payment_amount_coalesced ELSE 0 END) AS valid_revenue,
        AVG(CASE WHEN payment_amount > 0 THEN payment_amount END) AS avg_revenue_per_transaction,

        -- Energy Metrics
        SUM(energy_kwh_coalesced) AS total_energy_kwh,
        SUM(CASE WHEN is_valid_transaction = TRUE THEN energy_kwh_coalesced ELSE 0 END) AS valid_energy_kwh,
        AVG(CASE WHEN energy_kwh > 0 THEN energy_kwh END) AS avg_energy_per_transaction,

        -- Duration Metrics
        SUM(duration_minutes_coalesced) AS total_duration_minutes,
        AVG(CASE WHEN duration_minutes > 0 THEN duration_minutes END) AS avg_duration_minutes,

        -- Transaction Counts
        COUNT(*) AS total_transactions,
        SUM(completed_transaction_count) AS completed_transactions,
        SUM(failed_transaction_count) AS failed_transactions,
        SUM(CASE WHEN is_valid_transaction = TRUE THEN 1 ELSE 0 END) AS valid_transactions,

        -- Active Entity Counts
        COUNT(DISTINCT user_key) AS active_users,
        COUNT(DISTINCT charger_key) AS active_chargers,

        -- Data Quality Metrics
        SUM(CASE WHEN has_any_quality_issue = TRUE THEN 1 ELSE 0 END) AS transactions_with_issues,
        SUM(CASE WHEN is_missing_payment = TRUE THEN 1 ELSE 0 END) AS transactions_missing_payment,
        SUM(CASE WHEN is_invalid_time_range = TRUE THEN 1 ELSE 0 END) AS transactions_invalid_time,
        SUM(CASE WHEN is_missing_kwh = TRUE OR is_negative_kwh = TRUE THEN 1 ELSE 0 END) AS transactions_invalid_kwh,
        SUM(CASE WHEN is_outlier_kwh = TRUE OR is_outlier_duration = TRUE OR is_outlier_payment_amount = TRUE THEN 1 ELSE 0 END) AS transactions_with_outliers,

        -- Data Quality Percentages
        ROUND(
            100.0 * SUM(CASE WHEN is_valid_transaction = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) AS valid_transaction_pct,

        ROUND(
            100.0 * SUM(CASE WHEN has_any_quality_issue = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) AS issue_transaction_pct,

        ROUND(
            100.0 * SUM(CASE WHEN is_missing_payment = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) AS missing_payment_pct,

        -- Payment Method Distribution
        SUM(CASE WHEN payment_method = 'card' THEN 1 ELSE 0 END) AS card_transactions,
        SUM(CASE WHEN payment_method = 'RFID' THEN 1 ELSE 0 END) AS rfid_transactions,
        SUM(CASE WHEN payment_method = 'app_wallet' THEN 1 ELSE 0 END) AS app_wallet_transactions,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM fact_transactions
    WHERE transaction_date IS NOT NULL
    GROUP BY
        transaction_date,
        city_name,
        transaction_date_key
),

-- Add overall daily metrics (all cities combined)
daily_overall_metrics AS (
    SELECT
        -- Grain Keys
        transaction_date,
        '_ALL_CITIES_' AS city_name,

        -- Date Key for dimension join
        transaction_date_key,

        -- Revenue Metrics
        SUM(payment_amount_coalesced) AS total_revenue,
        SUM(CASE WHEN is_valid_transaction = TRUE THEN payment_amount_coalesced ELSE 0 END) AS valid_revenue,
        AVG(CASE WHEN payment_amount > 0 THEN payment_amount END) AS avg_revenue_per_transaction,

        -- Energy Metrics
        SUM(energy_kwh_coalesced) AS total_energy_kwh,
        SUM(CASE WHEN is_valid_transaction = TRUE THEN energy_kwh_coalesced ELSE 0 END) AS valid_energy_kwh,
        AVG(CASE WHEN energy_kwh > 0 THEN energy_kwh END) AS avg_energy_per_transaction,

        -- Duration Metrics
        SUM(duration_minutes_coalesced) AS total_duration_minutes,
        AVG(CASE WHEN duration_minutes > 0 THEN duration_minutes END) AS avg_duration_minutes,

        -- Transaction Counts
        COUNT(*) AS total_transactions,
        SUM(completed_transaction_count) AS completed_transactions,
        SUM(failed_transaction_count) AS failed_transactions,
        SUM(CASE WHEN is_valid_transaction = TRUE THEN 1 ELSE 0 END) AS valid_transactions,

        -- Active Entity Counts
        COUNT(DISTINCT user_key) AS active_users,
        COUNT(DISTINCT charger_key) AS active_chargers,

        -- Data Quality Metrics
        SUM(CASE WHEN has_any_quality_issue = TRUE THEN 1 ELSE 0 END) AS transactions_with_issues,
        SUM(CASE WHEN is_missing_payment = TRUE THEN 1 ELSE 0 END) AS transactions_missing_payment,
        SUM(CASE WHEN is_invalid_time_range = TRUE THEN 1 ELSE 0 END) AS transactions_invalid_time,
        SUM(CASE WHEN is_missing_kwh = TRUE OR is_negative_kwh = TRUE THEN 1 ELSE 0 END) AS transactions_invalid_kwh,
        SUM(CASE WHEN is_outlier_kwh = TRUE OR is_outlier_duration = TRUE OR is_outlier_payment_amount = TRUE THEN 1 ELSE 0 END) AS transactions_with_outliers,

        -- Data Quality Percentages
        ROUND(
            100.0 * SUM(CASE WHEN is_valid_transaction = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) AS valid_transaction_pct,

        ROUND(
            100.0 * SUM(CASE WHEN has_any_quality_issue = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) AS issue_transaction_pct,

        ROUND(
            100.0 * SUM(CASE WHEN is_missing_payment = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) AS missing_payment_pct,

        -- Payment Method Distribution
        SUM(CASE WHEN payment_method = 'card' THEN 1 ELSE 0 END) AS card_transactions,
        SUM(CASE WHEN payment_method = 'RFID' THEN 1 ELSE 0 END) AS rfid_transactions,
        SUM(CASE WHEN payment_method = 'app_wallet' THEN 1 ELSE 0 END) AS app_wallet_transactions,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM fact_transactions
    WHERE transaction_date IS NOT NULL
    GROUP BY
        transaction_date,
        transaction_date_key
)

SELECT * FROM daily_city_metrics
UNION ALL
SELECT * FROM daily_overall_metrics
