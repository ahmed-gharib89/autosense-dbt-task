/*
    Semantic Layer: Date Dimension

    Role-playing dimension for time intelligence analysis.
    Supports dashboard filters by day, week, month, quarter, and year.
    Uses dbt_utils.date_spine to generate a complete date range.
*/

{{ config(
    materialized='table',
    tags=['semantic', 'dimension', 'date']
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

date_dimension AS (
    SELECT
        -- Primary Key (surrogate key using YYYYMMDD format)
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,

        -- Natural Key
        date_day AS date_actual,

        -- Day Attributes
        DAYOFWEEK(date_day) AS day_of_week,          -- 0=Sunday, 6=Saturday
        DAYOFMONTH(date_day) AS day_of_month,
        DAYOFYEAR(date_day) AS day_of_year,
        DAYNAME(date_day) AS day_name,                -- Mon, Tue, Wed, etc.
        DECODE(DAYOFWEEK(date_day),
            0, 'Sunday',
            1, 'Monday',
            2, 'Tuesday',
            3, 'Wednesday',
            4, 'Thursday',
            5, 'Friday',
            6, 'Saturday'
        ) AS day_name_full,

        -- Week Attributes
        WEEKOFYEAR(date_day) AS week_of_year,
        DATE_TRUNC('week', date_day) AS week_start_date,
        DATEADD('day', 6, DATE_TRUNC('week', date_day)) AS week_end_date,

        -- Month Attributes
        MONTH(date_day) AS month_number,
        MONTHNAME(date_day) AS month_name,            -- Jan, Feb, Mar, etc.
        TO_CHAR(date_day, 'Month') AS month_name_full,
        DATE_TRUNC('month', date_day) AS month_start_date,
        LAST_DAY(date_day) AS month_end_date,

        -- Quarter Attributes
        QUARTER(date_day) AS quarter_number,
        'Q' || QUARTER(date_day) AS quarter_name,
        DATE_TRUNC('quarter', date_day) AS quarter_start_date,
        LAST_DAY(DATEADD('month', 2, DATE_TRUNC('quarter', date_day))) AS quarter_end_date,

        -- Year Attributes
        YEAR(date_day) AS year_number,
        DATE_TRUNC('year', date_day) AS year_start_date,
        LAST_DAY(DATEADD('month', 11, DATE_TRUNC('year', date_day))) AS year_end_date,

        -- Fiscal Year (assuming fiscal year = calendar year, adjust if needed)
        YEAR(date_day) AS fiscal_year,
        QUARTER(date_day) AS fiscal_quarter,

        -- Business Day Flags
        CASE
            WHEN DAYOFWEEK(date_day) IN (0, 6) THEN FALSE
            ELSE TRUE
        END AS is_weekday,

        CASE
            WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        -- Relative Date Flags (useful for dashboard filters)
        CASE WHEN date_day = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN date_day = DATEADD('day', -1, CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_yesterday,
        CASE WHEN DATE_TRUNC('week', date_day) = DATE_TRUNC('week', CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_week,
        CASE WHEN DATE_TRUNC('month', date_day) = DATE_TRUNC('month', CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_month,
        CASE WHEN DATE_TRUNC('quarter', date_day) = DATE_TRUNC('quarter', CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_quarter,
        CASE WHEN DATE_TRUNC('year', date_day) = DATE_TRUNC('year', CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_year,

        -- Previous Period Flags
        CASE WHEN DATE_TRUNC('month', date_day) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN TRUE ELSE FALSE END AS is_previous_month,
        CASE WHEN DATE_TRUNC('quarter', date_day) = DATE_TRUNC('quarter', DATEADD('quarter', -1, CURRENT_DATE())) THEN TRUE ELSE FALSE END AS is_previous_quarter,

        -- Rolling Period Flags
        CASE WHEN date_day >= DATEADD('day', -7, CURRENT_DATE()) AND date_day < CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_last_7_days,
        CASE WHEN date_day >= DATEADD('day', -30, CURRENT_DATE()) AND date_day < CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_last_30_days,
        CASE WHEN date_day >= DATEADD('day', -90, CURRENT_DATE()) AND date_day < CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_last_90_days,

        -- Metadata
        CURRENT_TIMESTAMP() AS _dbt_updated_at

    FROM date_spine
)

SELECT * FROM date_dimension
