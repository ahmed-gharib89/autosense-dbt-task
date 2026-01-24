/*
    Curated Layer: Transactions

    Cleans transaction data with data quality flags:
    - Flags invalid time ranges (end_time < start_time)
    - Flags negative kWh consumption
    - Flags statistical outliers in kWh_consumed and charging_duration using IQR method
    - Calculates charging_duration in minutes
*/

WITH transactions AS (
    SELECT
        *,
        -- Calculate charging duration in minutes (only for valid time ranges)
        CASE
            WHEN end_time IS NOT NULL AND start_time IS NOT NULL AND end_time >= start_time
            THEN DATEDIFF('minute', start_time, end_time)
            ELSE NULL
        END AS charging_duration_minutes
    FROM {{ ref('stg_transactions') }}
),

-- Calculate IQR bounds for kWh consumption outlier detection
kwh_stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY kwh_consumed) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY kwh_consumed) AS q3
    FROM transactions
    WHERE kwh_consumed > 0  -- Only consider positive values for IQR
),

kwh_iqr_bounds AS (
    SELECT
        q1,
        q3,
        q3 - q1 AS iqr,
        q1 - 1.5 * (q3 - q1) AS lower_bound,
        q3 + 1.5 * (q3 - q1) AS upper_bound
    FROM kwh_stats
),

-- Calculate IQR bounds for charging duration outlier detection
duration_stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY charging_duration_minutes) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY charging_duration_minutes) AS q3
    FROM transactions
    WHERE charging_duration_minutes > 0  -- Only consider valid positive durations
),

duration_iqr_bounds AS (
    SELECT
        q1,
        q3,
        q3 - q1 AS iqr,
        q1 - 1.5 * (q3 - q1) AS lower_bound,
        q3 + 1.5 * (q3 - q1) AS upper_bound
    FROM duration_stats
),

cleaned AS (
    SELECT
        -- Primary Key
        transactions.session_id,

        -- Foreign Keys
        transactions.user_id,
        transactions.charger_id,

        -- Transaction Details
        transactions.start_time,
        transactions.end_time,
        transactions.kwh_consumed,
        transactions.payment_method,
        transactions.status,

        -- Derived Fields
        transactions.charging_duration_minutes,

        -- Data Quality Flags: Time Validation
        CASE
            WHEN transactions.end_time IS NOT NULL
                 AND transactions.start_time IS NOT NULL
                 AND transactions.end_time < transactions.start_time
            THEN TRUE
            ELSE FALSE
        END AS is_invalid_time_range,

        -- Data Quality Flags: kWh Consumption
        CASE
            WHEN transactions.kwh_consumed IS NULL
            THEN TRUE
            ELSE FALSE
        END AS is_missing_kwh,

        CASE
            WHEN transactions.kwh_consumed IS NOT NULL AND transactions.kwh_consumed < 0
            THEN TRUE
            ELSE FALSE
        END AS is_negative_kwh,

        CASE
            WHEN transactions.kwh_consumed IS NOT NULL
                 AND transactions.kwh_consumed > 0
                 AND (
                     transactions.kwh_consumed < kwh_iqr_bounds.lower_bound
                     OR transactions.kwh_consumed > kwh_iqr_bounds.upper_bound
                 )
            THEN TRUE
            ELSE FALSE
        END AS is_outlier_kwh,

        -- Data Quality Flags: Charging Duration
        CASE
            WHEN transactions.charging_duration_minutes IS NOT NULL
                 AND transactions.charging_duration_minutes > 0
                 AND (
                     transactions.charging_duration_minutes < duration_iqr_bounds.lower_bound
                     OR transactions.charging_duration_minutes > duration_iqr_bounds.upper_bound
                 )
            THEN TRUE
            ELSE FALSE
        END AS is_outlier_duration,

        -- Statistical context (for analysis)
        kwh_iqr_bounds.lower_bound AS _kwh_outlier_lower_bound,
        kwh_iqr_bounds.upper_bound AS _kwh_outlier_upper_bound,
        duration_iqr_bounds.lower_bound AS _duration_outlier_lower_bound,
        duration_iqr_bounds.upper_bound AS _duration_outlier_upper_bound,

        -- Metadata
        transactions._loaded_at,
        transactions._source_file,
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM transactions
    CROSS JOIN kwh_iqr_bounds
    CROSS JOIN duration_iqr_bounds
)

SELECT * FROM cleaned
