# Retail Data Warehouse

A dbt project for a retail analytics data warehouse.

## Structure

```
models/
├── staging/      # raw data cleaning (hourly)
├── intermediate/ # business logic joins (hourly)
├── marts/        # dimension and fact tables (daily)
└── reporting/    # aggregated reports (daily)
```

## Setup

```bash
pip install dbt-bigquery  # or dbt-postgres, dbt-snowflake
cp profiles.yml.example ~/.dbt/profiles.yml
# edit profiles.yml with your credentials
dbt deps
dbt build
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

## Sources

Configure your source schema in `models/staging/_sources.yml` to point to your raw data tables.
