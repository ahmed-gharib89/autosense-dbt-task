-- =====================================================
-- Snowflake Raw Layer Setup Script
-- AutoSense EV Charging Data Pipeline
-- =====================================================
-- This script creates the database, schema, stages,
-- and tables for loading JSON data files.
-- Run this BEFORE executing dbt models.
-- =====================================================

-- =====================================================
-- STEP 1: Create Database and Schema
-- =====================================================
CREATE DATABASE IF NOT EXISTS AUTOSENSE;
USE DATABASE AUTOSENSE;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA RAW_DATA;

-- =====================================================
-- STEP 2: Create File Format for JSON
-- =====================================================
CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE;

-- =====================================================
-- STEP 3: Create Internal Stage for JSON Files
-- =====================================================
-- Internal Stage
CREATE OR REPLACE STAGE json_stage
    FILE_FORMAT = json_format;

-- =====================================================
-- STEP 4: Create Raw Tables with VARIANT Columns
-- =====================================================

-- Users Raw Table
CREATE OR REPLACE TABLE raw_users (
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);

-- Chargers Raw Table
CREATE OR REPLACE TABLE raw_chargers (
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);

-- Transactions Raw Table
CREATE OR REPLACE TABLE raw_transactions (
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);

-- Payments Raw Table
CREATE OR REPLACE TABLE raw_payments (
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);

-- =====================================================
-- STEP 5: Upload Files to Stage (run from SnowSQL CLI)
-- =====================================================
--
PUT file://./data/users.json @json_stage/users/ AUTO_COMPRESS=TRUE;
PUT file://./data/chargers.json @json_stage/chargers/ AUTO_COMPRESS=TRUE;
PUT file://./data/transactions.json @json_stage/transactions/ AUTO_COMPRESS=TRUE;
PUT file://./data/payments.json @json_stage/payments/ AUTO_COMPRESS=TRUE;

-- =====================================================
-- STEP 6: Load Data from Stage to Raw Tables
-- =====================================================

-- Load Users
COPY INTO raw_users (raw_data, _source_file)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @json_stage/users/
)
FILE_FORMAT = json_format
ON_ERROR = 'CONTINUE';

-- Load Chargers
COPY INTO raw_chargers (raw_data, _source_file)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @json_stage/chargers/
)
FILE_FORMAT = json_format
ON_ERROR = 'CONTINUE';

-- Load Transactions
COPY INTO raw_transactions (raw_data, _source_file)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @json_stage/transactions/
)
FILE_FORMAT = json_format
ON_ERROR = 'CONTINUE';

-- Load Payments
COPY INTO raw_payments (raw_data, _source_file)
FROM (
    SELECT
        $1,
        METADATA$FILENAME
    FROM @json_stage/payments/
)
FILE_FORMAT = json_format
ON_ERROR = 'CONTINUE';

-- =====================================================
-- STEP 7: Verify Data Load
-- =====================================================
SELECT 'raw_users' AS table_name, COUNT(*) AS row_count FROM raw_users
UNION ALL
SELECT 'raw_chargers', COUNT(*) FROM raw_chargers
UNION ALL
SELECT 'raw_transactions', COUNT(*) FROM raw_transactions
UNION ALL
SELECT 'raw_payments', COUNT(*) FROM raw_payments;

-- =====================================================
-- STEP 8: Grant Permissions to dbt Role (if needed)
-- =====================================================
-- Uncomment and modify as needed:
GRANT USAGE ON DATABASE AUTOSENSE TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA AUTOSENSE.RAW_DATA TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA AUTOSENSE.RAW_DATA TO ROLE DBT_ROLE;
