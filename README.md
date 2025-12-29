# Retail Data Warehouse

A dbt-core project for a retail analytics data warehouse.

## Setup

```bash
pip install dbt-core dbt-postgres  # or dbt-snowflake, dbt-bigquery, dbt-redshift
cp profiles.yml.example ~/.dbt/profiles.yml
# edit profiles.yml with your credentials
dbt deps
dbt build
```

## Structure

```
    ┌─────────────────────────────────────────┐
    │  REPORTING    rpt_*                     │  <- cheese topping (served to users)
    │  daily sales, top customers, categories │
    ├─────────────────────────────────────────┤
    │  MARTS        dim_* / fct_*             │  <- ricotta layer (business-ready)
    │  dimensions & facts                     │
    ├─────────────────────────────────────────┤
    │  INTERMEDIATE int_*                     │  <- meat sauce (joins & logic)
    │  enriched orders, enriched items        │
    ├─────────────────────────────────────────┤
    │  STAGING      stg_*                     │  <- pasta sheets (cleaned raw data)
    │  customers, products, orders, items     │
    ├─────────────────────────────────────────┤
    │  SOURCES      raw.*                     │  <- the plate (raw database tables)
    │  raw.customers, raw.products, etc       │
    └─────────────────────────────────────────┘
```

```
models/
├── staging/      # raw data cleaning (hourly)
├── intermediate/ # business logic joins (hourly)
├── marts/        # dimension and fact tables (daily)
└── reporting/    # aggregated reports (daily)
```

## Tags

Models are tagged by refresh cadence:
- `hourly` - staging and intermediate models
- `daily` - marts and reporting models

Run by tag:
```bash
dbt run --select tag:hourly
dbt run --select tag:daily
```

## Models

### Staging
- `stg_customers` - cleaned customer data
- `stg_products` - cleaned product data
- `stg_orders` - cleaned order data
- `stg_order_items` - cleaned line items

### Marts
- `dim_customers` - customer dimension with lifetime metrics
- `dim_products` - product dimension with sales metrics
- `fct_orders` - order facts
- `fct_order_items` - line item facts

### Reporting
- `rpt_daily_sales` - daily revenue summary
- `rpt_category_sales` - sales by product category
- `rpt_top_customers` - customer ranking by spend

## CI/CD

GitHub Actions workflows:
- `dbt_hourly.yml` - runs hourly tagged models every hour
- `dbt_daily.yml` - runs daily tagged models at 6am UTC
- `dbt_pr.yml` - compiles on pull requests

Set these secrets in your repo:
- `DBT_HOST`
- `DBT_USER`
- `DBT_PASSWORD`
- `DBT_DATABASE`

## Sources

Configure your source schema in `models/staging/_sources.yml` to point to your raw data tables.
