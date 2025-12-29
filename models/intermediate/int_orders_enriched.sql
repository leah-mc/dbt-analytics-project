with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_totals as (
    select
        order_id,
        sum(line_total) as order_total,
        count(*) as item_count
    from {{ ref('int_order_items_enriched') }}
    group by order_id
),

enriched as (
    select
        o.order_id,
        o.customer_id,
        c.customer_name,
        c.email,
        o.order_status,
        o.order_date,
        coalesce(ot.order_total, 0) as order_total,
        coalesce(ot.item_count, 0) as item_count
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join order_totals ot on o.order_id = ot.order_id
)

select * from enriched
