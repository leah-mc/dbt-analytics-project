# Retail Data Warehouse

A dbt project for a retail analytics data warehouse.

## Structure

```
models/
├── staging/      # raw data cleaning
├── intermediate/ # business logic joins
├── marts/        # dimension and fact tables
└── reporting/    # aggregated reports
```

## Setup

```bash
pip install dbt-bigquery  # or dbt-postgres, dbt-snowflake
dbt deps
dbt build
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

## Sources

Configure your source schema in `models/staging/_sources.yml` to point to your raw data tables.
