with items as (
    select * from {{ ref('fct_order_items') }}
),

by_category as (
    select
        category,
        count(distinct order_id) as order_count,
        sum(quantity) as units_sold,
        sum(line_total) as revenue
    from items
    group by category
)

select * from by_category
order by revenue desc
