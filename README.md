# AutoSense EV Charging Analytics Pipeline

A dbt-based analytics pipeline for EV charging operations, transforming raw JSON data into analytics-ready dimensional models optimized for Tableau dashboards.

## Table of Contents

- [Overview](#overview)
- [Setup Instructions](#setup-instructions)
- [Project Architecture](#project-architecture)
- [Data Model](#data-model)
- [Data Quality Findings](#data-quality-findings)
- [Performance Considerations](#performance-considerations)
- [Production Readiness](#production-readiness)

---

## Overview

This project implements a complete EV charging analytics pipeline using:

- **dbt (Data Build Tool)**: SQL-based transformation framework
- **Snowflake**: Cloud data warehouse
- **Python/Jupyter**: Exploratory data analysis
- **Tableau**: Business intelligence dashboards

### Key Features

- Three-layer medallion architecture (Bronze → Silver → Gold)
- Star schema dimensional model for analytics
- Comprehensive data quality validation and flagging
- Pre-aggregated metrics for dashboard performance

---

## Setup Instructions

### Prerequisites

- Python 3.9+
- dbt-core and dbt-snowflake adapter
- Snowflake account with appropriate permissions
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/ahmed-gharib89/autosense-dbt-task.git
cd autosense-dbt-task
```

### 2. Create Python Environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure dbt Profile

Create `~/.dbt/profiles.yml`:

```yaml
autosense_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: <your_user>
      password: <your_password>
      role: <your_role>
      database: AUTOSENSE
      warehouse: <your_warehouse>
      schema: DEV
      threads: 4
```

### 4. Load Raw Data into Snowflake

Run the setup script to create database objects and load JSON files:

```bash
# Upload JSON files to Snowflake stage
snowsql -f snowflake/setup_raw_layer.sql
```

### 5. Install dbt Packages

```bash
dbt deps
```

### 6. Build the Pipeline

```bash
# Run all models
dbt build

# Or run specific layers
dbt build --select stage      # Bronze layer
dbt build --select curated    # Silver layer
dbt build --select semantic   # Gold layer (Star Schema)
```

### 7. Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

---

## Project Architecture

### Pipeline Overview

```mermaid
flowchart LR
    subgraph Sources["Data Sources"]
        J1[("users.json")]
        J2[("chargers.json")]
        J3[("transactions.json")]
        J4[("payments.json")]
    end

    subgraph Snowflake["Snowflake Data Warehouse"]
        subgraph Raw["RAW_DATA Schema"]
            R1[("raw_users")]
            R2[("raw_chargers")]
            R3[("raw_transactions")]
            R4[("raw_payments")]
        end

        subgraph Bronze["STG Schema (Bronze)"]
            S1["stg_users"]
            S2["stg_chargers"]
            S3["stg_transactions"]
            S4["stg_payments"]
        end

        subgraph Silver["CURATED Schema (Silver)"]
            C1["cur_users"]
            C2["cur_chargers"]
            C3["cur_transactions"]
            C4["cur_payments"]
        end

        subgraph Gold["SEMANTIC Schema (Gold)"]
            D1["dim_users"]
            D2["dim_chargers"]
            D3["dim_cities"]
            D4["dim_dates"]
            F1["fact_transactions"]
            F2["fact_daily_metrics"]
            F3["fact_charger_performance"]
        end
    end

    subgraph BI["Business Intelligence"]
        T["Tableau Dashboard"]
    end

    J1 --> R1
    J2 --> R2
    J3 --> R3
    J4 --> R4

    R1 --> S1
    R2 --> S2
    R3 --> S3
    R4 --> S4

    S1 --> C1
    S2 --> C2
    S3 --> C3
    S4 --> C4

    C1 --> D1
    C2 --> D2
    C2 --> D3
    C3 --> F1
    C4 --> F1

    D1 --> F1
    D2 --> F1
    D4 --> F1
    F1 --> F2
    F1 --> F3

    F1 --> T
    F2 --> T
    F3 --> T
```

### Layer Descriptions

| Layer      | Schema     | Purpose                                        | Materialization |
| ---------- | ---------- | ---------------------------------------------- | --------------- |
| **Bronze** | `STG`      | Parse JSON, type casting, column renaming      | Table           |
| **Silver** | `CURATED`  | Data cleansing, validation, quality flags      | Table           |
| **Gold**   | `SEMANTIC` | Star schema dimensions and facts for analytics | Table           |

### Directory Structure

```
autosense-dbt-task/
├── analyses/              # Jupyter notebooks for EDA
│   └── eda.ipynb         # Data quality analysis notebook
├── data/                  # Source JSON files
├── models/
│   ├── stage/            # Bronze layer - staging models
│   ├── curated/          # Silver layer - cleaned models
│   └── semantic/         # Gold layer - star schema
├── macros/               # Custom dbt macros
├── seeds/                # Reference data (city mapping)
├── snapshots/            # SCD Type 2 tracking
├── tests/                # Custom data tests
├── snowflake/            # Snowflake setup scripts
└── dbt_project.yml       # dbt configuration
```

---

## Data Model

### Star Schema (Semantic Layer)

```mermaid
erDiagram
    dim_dates ||--o{ fact_transactions : "transaction_date_key"
    dim_users ||--o{ fact_transactions : "user_key"
    dim_chargers ||--o{ fact_transactions : "charger_key"
    dim_cities ||--o{ dim_chargers : "city_name"

    dim_dates {
        int date_key PK "YYYYMMDD format"
        date date_actual
        int day_of_week
        int month_number
        int quarter_number
        int year_number
        boolean is_weekend
        boolean is_current_month
        boolean is_last_90_days
    }

    dim_users {
        string user_key PK
        string user_id
        string user_name
        string user_email
        string user_tier
        string user_tenure_band
        timestamp user_created_at
    }

    dim_chargers {
        string charger_key PK
        string charger_id
        string charger_city
        float charger_latitude
        float charger_longitude
        string charger_age_band
        boolean has_data_quality_issue
        string data_quality_status
    }

    dim_cities {
        string city_key PK
        string city_name
        int total_chargers
        string market_size_tier
        float city_center_latitude
        float city_center_longitude
    }

    fact_transactions {
        string session_id PK
        int transaction_date_key FK
        string user_key FK
        string charger_key FK
        string city_name
        float energy_kwh
        float payment_amount
        int duration_minutes
        string payment_method
        string transaction_status
        boolean is_valid_transaction
        boolean has_any_quality_issue
    }

    fact_daily_metrics {
        date transaction_date PK
        string city_name PK
        float total_revenue
        float total_energy_kwh
        int total_transactions
        int active_users
        int active_chargers
        float valid_transaction_pct
        float missing_payment_pct
    }

    fact_charger_performance {
        string charger_key PK
        int total_transactions
        float total_revenue
        float avg_revenue_per_transaction
        int transaction_volume_quartile
        int revenue_per_txn_quartile
        string performance_classification
    }

    fact_transactions ||--o{ fact_daily_metrics : "aggregates"
    fact_transactions ||--o{ fact_charger_performance : "aggregates"
```

### Model Descriptions

#### Dimension Tables

| Model          | Description                               | Key Attributes                                   |
| -------------- | ----------------------------------------- | ------------------------------------------------ |
| `dim_dates`    | Date dimension with time intelligence     | Fiscal periods, relative flags (is_last_90_days) |
| `dim_users`    | User dimension with lifecycle analysis    | Tier, tenure bands, created date                 |
| `dim_chargers` | Charger dimension with location & quality | City, coordinates, age bands, DQ flags           |
| `dim_cities`   | City dimension for geographic analysis    | Market size tier, charger counts                 |

#### Fact Tables

| Model                      | Grain              | Key Measures                                 |
| -------------------------- | ------------------ | -------------------------------------------- |
| `fact_transactions`        | 1 row per session  | energy_kwh, payment_amount, duration_minutes |
| `fact_daily_metrics`       | 1 row per day/city | Aggregated KPIs, DQ percentages              |
| `fact_charger_performance` | 1 row per charger  | Performance quartiles, classification        |

---

## Data Quality Findings

### Summary of Issues Found

The source data contains intentional quality issues. Here's what was identified and how each is handled:

```mermaid
flowchart TD
    subgraph Chargers["Charger Data Issues"]
        C1["City Name Variants"]
        C2["Invalid GPS Coordinates"]
        C3["Missing Locations"]
    end

    subgraph Transactions["Transaction Data Issues"]
        T1["Invalid Time Ranges"]
        T2["Negative kWh Values"]
        T3["Outlier kWh Values"]
        T4["Outlier Durations"]
    end

    subgraph Payments["Payment Data Issues"]
        P1["Missing Payment Records"]
        P2["Zero/Negative Amounts"]
        P3["Outlier Amounts"]
    end

    subgraph Handling["Handling Strategy"]
        H1["Seed Mapping Table"]
        H2["Switzerland Bounds Check"]
        H3["Quality Flags"]
        H4["IQR Outlier Detection"]
        H5["Left Join with NULL handling"]
    end

    C1 --> H1
    C2 --> H2
    C3 --> H3
    T1 --> H3
    T2 --> H3
    T3 --> H4
    T4 --> H4
    P1 --> H5
    P2 --> H3
    P3 --> H4
```

### Detailed Findings

#### 1. Location Data Issues

| Issue                   | Count                                        | Detection Method                                            | Handling                                           |
| ----------------------- | -------------------------------------------- | ----------------------------------------------------------- | -------------------------------------------------- |
| **City name variants**  | Multiple spellings (Zurich, Zuerich, Zürich) | Distinct value analysis                                     | `city_name_mapping` seed table for standardization |
| **Invalid coordinates** | Outside Switzerland bounds                   | Bounding box validation (lat: 45.82-47.81, lon: 5.96-10.49) | `is_invalid_location` flag                         |
| **Missing coordinates** | NULL latitude/longitude                      | NULL check                                                  | `is_invalid_location` flag                         |

#### 2. Transaction Data Issues

| Issue                     | Detection Method                        | Handling                                    |
| ------------------------- | --------------------------------------- | ------------------------------------------- |
| **End time < Start time** | Timestamp comparison                    | `is_invalid_time_range` flag, NULL duration |
| **Negative kWh consumed** | Value < 0 check                         | `is_negative_kwh` flag                      |
| **Outlier kWh values**    | IQR method (Q1 - 1.5×IQR, Q3 + 1.5×IQR) | `is_outlier_kwh` flag                       |
| **Outlier durations**     | IQR method                              | `is_outlier_duration` flag                  |

#### 3. Payment Data Issues

| Issue                       | Detection Method       | Handling                                       |
| --------------------------- | ---------------------- | ---------------------------------------------- |
| **Missing payment records** | LEFT JOIN returns NULL | `is_missing_payment` flag in fact_transactions |
| **Zero/negative amounts**   | Value ≤ 0 check        | `is_invalid_amount` flag                       |
| **Outlier amounts**         | IQR method             | `is_outlier_amount` flag                       |

### Data Quality Approach

**Philosophy**: Flag, don't filter. All records are preserved with quality flags, enabling:

1. **Inclusive reporting**: Show all data, filter in dashboards as needed
2. **Quality monitoring**: Track DQ trends over time
3. **Root cause analysis**: Investigate issues without data loss
4. **Flexible aggregation**: `is_valid_transaction` flag for clean metrics

---

## Performance Considerations

### Materialization Strategy

| Layer        | Materialization | Rationale                                       |
| ------------ | --------------- | ----------------------------------------------- |
| **Stage**    | Table           | Parsed once, read many times by curated layer   |
| **Curated**  | Table           | Complex IQR calculations, quality flag logic    |
| **Semantic** | Table           | Dimensions are small; facts optimized for joins |

### Pre-Aggregated Tables

Two aggregated fact tables reduce Tableau query complexity:

1. **`fact_daily_metrics`**: Pre-computed daily KPIs by city
   - Eliminates complex GROUP BY in dashboards
   - Includes `_ALL_CITIES_` rows for totals

2. **`fact_charger_performance`**: Lifetime charger metrics
   - Performance quartiles pre-calculated
   - Classification logic computed once

### Query Optimization Techniques

- **Denormalized city_name** in fact_transactions for direct filtering
- **Date keys as integers** (YYYYMMDD) for efficient range filtering
- **Boolean quality flags** instead of string status codes
- **Coalesced columns** (\_coalesced suffix) for safe SUM aggregations

### Snowflake-Specific Optimizations

```sql
-- Clustering could be added for large fact tables:
-- ALTER TABLE semantic.fact_transactions CLUSTER BY (transaction_date);

-- Warehouse recommendations:
-- - X-Small for development
-- - Small/Medium for production dashboards
-- - Auto-suspend after 5 minutes
```

---

## Production Readiness

### Current State Assessment

| Aspect                     | Status         | Notes                           |
| -------------------------- | -------------- | ------------------------------- |
| **Data Quality Testing**   | ✅ Ready       | Comprehensive schema.yml tests  |
| **Documentation**          | ✅ Ready       | All models documented           |
| **Materialization**        | ✅ Ready       | Appropriate for data volumes    |
| **Error Handling**         | ✅ Ready       | Quality flags preserve all data |
| **Incremental Processing** | ⚠️ Enhancement | Currently full refresh          |
| **CI/CD**                  | ⚠️ Enhancement | Not configured                  |
| **Monitoring**             | ⚠️ Enhancement | Basic logging only              |

### Recommended Production Enhancements

#### 1. Incremental Models

For large fact tables, convert to incremental:

```sql
{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge'
) }}

SELECT ...
{% if is_incremental() %}
WHERE start_time > (SELECT MAX(start_time) FROM {{ this }})
{% endif %}
```

#### 2. CI/CD Pipeline

```yaml
# .github/workflows/dbt.yml
name: dbt CI
on: [push, pull_request]
jobs:
  dbt-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - run: dbt deps
      - run: dbt build --select state:modified+
      - run: dbt test
```

#### 3. Data Freshness Monitoring

Add source freshness checks:

```yaml
# models/sources.yml
sources:
  - name: raw
    freshness:
      warn_after: { count: 24, period: hour }
      error_after: { count: 48, period: hour }
```

#### 4. Alerting Integration

- Configure Snowflake alerts for failed loads
- Integrate with PagerDuty/Slack for DQ threshold breaches
- Monitor `valid_transaction_pct` dropping below threshold

### Security Considerations

- ✅ No credentials in code
- ✅ Role-based access via Snowflake RBAC
- ⚠️ Recommend: Column-level encryption for PII (email)
- ⚠️ Recommend: Row-level security for multi-tenant scenarios

---

## Running Tests

```bash
# Run all tests
dbt test

# Run tests for specific layer
dbt test --select semantic

# Run only data tests (skip schema tests)
dbt test --select test_type:data
```

### Test Coverage

- **Primary Key Tests**: unique + not_null on all dimension/fact keys
- **Referential Integrity**: relationships tests between facts and dimensions
- **Accepted Values**: payment_method, transaction_status, user_tier
- **Custom Tests**: Quality flag validations

---

## License

This project is part of the AutoSense Data Engineer Assessment.

## Author

Ahmed Gharib - [GitHub](https://github.com/ahmed-gharib89)
