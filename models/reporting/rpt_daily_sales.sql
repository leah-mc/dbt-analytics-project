with orders as (
    select * from {{ ref('fct_orders') }}
),

daily as (
    select
        cast(order_date as date) as sale_date,
        count(*) as total_orders,
        sum(order_total) as total_revenue,
        avg(order_total) as avg_order_value
    from orders
    group by 1
),

with_comparisons as (
    select
        sale_date,
        total_orders,
        total_revenue,
        avg_order_value,

        -- previous day metrics using lag
        lag(total_orders) over (order by sale_date) as prev_day_orders,
        lag(total_revenue) over (order by sale_date) as prev_day_revenue,

        -- day-over-day changes
        total_orders - lag(total_orders) over (order by sale_date) as orders_dod_change,
        total_revenue - lag(total_revenue) over (order by sale_date) as revenue_dod_change,

        -- day-over-day percent changes
        round(
            safe_divide(
                total_orders - lag(total_orders) over (order by sale_date),
                lag(total_orders) over (order by sale_date)
            ) * 100,
            2
        ) as orders_dod_pct_change,
        round(
            safe_divide(
                total_revenue - lag(total_revenue) over (order by sale_date),
                lag(total_revenue) over (order by sale_date)
            ) * 100,
            2
        ) as revenue_dod_pct_change,

        -- 7-day rolling average
        round(
            avg(total_revenue) over (
                order by sale_date
                rows between 6 preceding and current row
            ),
            2
        ) as revenue_7day_avg

    from daily
)

select * from with_comparisons
order by sale_date
