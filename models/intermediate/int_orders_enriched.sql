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

-- get first touch attribution
first_touch as (
    select
        order_id,
        utm_source as first_touch_source,
        utm_medium as first_touch_medium,
        utm_campaign as first_touch_campaign
    from {{ ref('int_sessions_to_orders') }}
    where touchpoint_position = 'first'
),

-- get last touch attribution
last_touch as (
    select
        order_id,
        utm_source as last_touch_source,
        utm_medium as last_touch_medium,
        utm_campaign as last_touch_campaign
    from {{ ref('int_sessions_to_orders') }}
    where touchpoint_position = 'last'
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
        coalesce(ot.item_count, 0) as item_count,
        ft.first_touch_source,
        ft.first_touch_medium,
        ft.first_touch_campaign,
        lt.last_touch_source,
        lt.last_touch_medium,
        lt.last_touch_campaign
    from orders as o
    left join customers as c on o.customer_id = c.customer_id
    left join order_totals as ot on o.order_id = ot.order_id
    left join first_touch as ft on o.order_id = ft.order_id
    left join last_touch as lt on o.order_id = lt.order_id
)

select * from enriched
