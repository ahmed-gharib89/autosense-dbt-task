/*
    Curated Layer: Payments

    Cleans payment data with data quality flags:
    - Flags zero or negative payment amounts
    - Flags statistical outliers using IQR method (Q1 - 1.5*IQR, Q3 + 1.5*IQR)
*/

WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

-- Calculate IQR bounds for outlier detection
payment_stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) AS q3
    FROM payments
    WHERE amount > 0  -- Only consider positive amounts for IQR calculation
),

iqr_bounds AS (
    SELECT
        q1,
        q3,
        q3 - q1 AS iqr,
        q1 - 1.5 * (q3 - q1) AS lower_bound,
        q3 + 1.5 * (q3 - q1) AS upper_bound
    FROM payment_stats
),

cleaned AS (
    SELECT
        -- Primary Key
        payments.payment_id,

        -- Foreign Keys
        payments.session_id,
        payments.user_id,

        -- Payment Details
        payments.amount,
        payments.currency,

        -- Data Quality Flags
        CASE
            WHEN payments.amount IS NULL
            THEN TRUE
            ELSE FALSE
        END AS is_missing_amount,

        CASE
            WHEN payments.amount IS NOT NULL AND payments.amount <= 0
            THEN TRUE
            ELSE FALSE
        END AS is_invalid_amount,

        CASE
            WHEN payments.amount IS NOT NULL
                 AND payments.amount > 0
                 AND (
                     payments.amount < iqr_bounds.lower_bound
                     OR payments.amount > iqr_bounds.upper_bound
                 )
            THEN TRUE
            ELSE FALSE
        END AS is_outlier_amount,

        -- Statistical context (for analysis)
        iqr_bounds.lower_bound AS _outlier_lower_bound,
        iqr_bounds.upper_bound AS _outlier_upper_bound,

        -- Metadata
        payments._loaded_at,
        payments._source_file,
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM payments
    CROSS JOIN iqr_bounds
)

SELECT * FROM cleaned
