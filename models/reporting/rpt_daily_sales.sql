with orders as (
    select * from {{ ref('fct_orders') }}
),

daily as (
    select
        date(order_date) as sale_date,
        count(*) as total_orders,
        sum(order_total) as total_revenue,
        avg(order_total) as avg_order_value
    from orders
    group by date(order_date)
)

select * from daily
order by sale_date
