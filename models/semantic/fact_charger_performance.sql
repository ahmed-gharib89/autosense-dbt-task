/*
    Semantic Layer: Charger Performance Fact Table

    Pre-aggregated charger-level metrics for dashboard performance.
    Grain: One row per charger (lifetime aggregation).

    Supports dashboard requirements:
    - Charger Performance Matrix (transaction volume vs revenue per transaction)
    - Identifying underperforming chargers
*/

{{ config(
    materialized='table',
    tags=['semantic', 'fact', 'charger', 'performance']
) }}

WITH fact_transactions AS (
    SELECT * FROM {{ ref('fact_transactions') }}
),

dim_chargers AS (
    SELECT * FROM {{ ref('dim_chargers') }}
),

charger_performance AS (
    SELECT
        -- Charger Key
        ft.charger_key,

        -- Transaction Volume Metrics
        COUNT(*) AS total_transactions,
        SUM(ft.completed_transaction_count) AS completed_transactions,
        SUM(ft.failed_transaction_count) AS failed_transactions,
        SUM(CASE WHEN ft.is_valid_transaction = TRUE THEN 1 ELSE 0 END) AS valid_transactions,

        -- Revenue Metrics
        SUM(ft.payment_amount_coalesced) AS total_revenue,
        SUM(CASE WHEN ft.is_valid_transaction = TRUE THEN ft.payment_amount_coalesced ELSE 0 END) AS valid_revenue,

        -- Revenue Per Transaction (key metric for performance matrix)
        CASE
            WHEN COUNT(*) > 0
            THEN SUM(ft.payment_amount_coalesced) / COUNT(*)
            ELSE 0
        END AS avg_revenue_per_transaction,

        CASE
            WHEN SUM(CASE WHEN ft.is_valid_transaction = TRUE THEN 1 ELSE 0 END) > 0
            THEN SUM(CASE WHEN ft.is_valid_transaction = TRUE THEN ft.payment_amount_coalesced ELSE 0 END)
                 / SUM(CASE WHEN ft.is_valid_transaction = TRUE THEN 1 ELSE 0 END)
            ELSE 0
        END AS avg_valid_revenue_per_transaction,

        -- Energy Metrics
        SUM(ft.energy_kwh_coalesced) AS total_energy_kwh,
        AVG(CASE WHEN ft.energy_kwh > 0 THEN ft.energy_kwh END) AS avg_energy_per_transaction,

        -- Duration Metrics
        SUM(ft.duration_minutes_coalesced) AS total_duration_minutes,
        AVG(CASE WHEN ft.duration_minutes > 0 THEN ft.duration_minutes END) AS avg_duration_minutes,

        -- User Metrics
        COUNT(DISTINCT ft.user_key) AS unique_users,

        -- Time Range
        MIN(ft.transaction_date) AS first_transaction_date,
        MAX(ft.transaction_date) AS last_transaction_date,
        DATEDIFF('day', MIN(ft.transaction_date), MAX(ft.transaction_date)) + 1 AS active_days_span,

        -- Activity Metrics
        COUNT(DISTINCT ft.transaction_date) AS days_with_transactions,
        CASE
            WHEN DATEDIFF('day', MIN(ft.transaction_date), MAX(ft.transaction_date)) + 1 > 0
            THEN ROUND(
                100.0 * COUNT(DISTINCT ft.transaction_date)
                / (DATEDIFF('day', MIN(ft.transaction_date), MAX(ft.transaction_date)) + 1),
                2
            )
            ELSE 0
        END AS utilization_pct,

        -- Daily Averages
        CASE
            WHEN COUNT(DISTINCT ft.transaction_date) > 0
            THEN COUNT(*) / COUNT(DISTINCT ft.transaction_date)
            ELSE 0
        END AS avg_transactions_per_day,

        CASE
            WHEN COUNT(DISTINCT ft.transaction_date) > 0
            THEN SUM(ft.payment_amount_coalesced) / COUNT(DISTINCT ft.transaction_date)
            ELSE 0
        END AS avg_revenue_per_day,

        -- Data Quality Metrics
        SUM(CASE WHEN ft.has_any_quality_issue = TRUE THEN 1 ELSE 0 END) AS transactions_with_issues,
        ROUND(
            100.0 * SUM(CASE WHEN ft.is_valid_transaction = TRUE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) AS valid_transaction_pct,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM fact_transactions ft
    GROUP BY ft.charger_key
),

charger_performance_enriched AS (
    SELECT
        cp.*,

        -- Charger Attributes (denormalized for easier reporting)
        dc.charger_id,
        dc.charger_city,
        dc.charger_installed_date,
        dc.charger_age_days,
        dc.charger_age_band,
        dc.has_data_quality_issue AS charger_has_data_quality_issue,
        dc.data_quality_status AS charger_data_quality_status,

        -- Performance Quartiles (calculated across all chargers)
        NTILE(4) OVER (ORDER BY cp.total_transactions) AS transaction_volume_quartile,
        NTILE(4) OVER (ORDER BY cp.avg_revenue_per_transaction) AS revenue_per_txn_quartile,
        NTILE(4) OVER (ORDER BY cp.total_revenue) AS total_revenue_quartile,

        -- Performance Classification
        CASE
            WHEN NTILE(4) OVER (ORDER BY cp.total_transactions) = 4
                 AND NTILE(4) OVER (ORDER BY cp.avg_revenue_per_transaction) = 4
            THEN 'Star Performer'
            WHEN NTILE(4) OVER (ORDER BY cp.total_transactions) >= 3
                 AND NTILE(4) OVER (ORDER BY cp.avg_revenue_per_transaction) >= 3
            THEN 'High Performer'
            WHEN NTILE(4) OVER (ORDER BY cp.total_transactions) <= 2
                 AND NTILE(4) OVER (ORDER BY cp.avg_revenue_per_transaction) <= 2
            THEN 'Underperformer'
            WHEN NTILE(4) OVER (ORDER BY cp.total_transactions) >= 3
                 AND NTILE(4) OVER (ORDER BY cp.avg_revenue_per_transaction) <= 2
            THEN 'High Volume / Low Value'
            WHEN NTILE(4) OVER (ORDER BY cp.total_transactions) <= 2
                 AND NTILE(4) OVER (ORDER BY cp.avg_revenue_per_transaction) >= 3
            THEN 'Low Volume / High Value'
            ELSE 'Average'
        END AS performance_classification

    FROM charger_performance cp
    LEFT JOIN dim_chargers dc
        ON cp.charger_key = dc.charger_key
)

SELECT * FROM charger_performance_enriched
