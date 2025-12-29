-- u-shaped attribution by campaign
-- provides campaign-level attribution metrics

with sessions_to_orders as (
    select * from {{ ref('int_sessions_to_orders') }}
),

orders as (
    select
        order_id,
        order_total
    from {{ ref('int_orders_enriched') }}
),

-- calculate u-shaped attribution weights
with_attribution as (
    select
        sto.order_id,
        sto.session_id,
        sto.utm_source,
        sto.utm_medium,
        sto.utm_campaign,
        sto.touchpoint_position,
        sto.total_touchpoints,
        o.order_total,
        case
            when sto.total_touchpoints = 1 then 1.0
            when sto.total_touchpoints = 2 then 0.5
            when sto.touchpoint_position = 'first' then 0.4
            when sto.touchpoint_position = 'last' then 0.4
            else 0.2 / (sto.total_touchpoints - 2)
        end as attribution_weight,
        case
            when sto.total_touchpoints = 1 then o.order_total
            when sto.total_touchpoints = 2 then o.order_total * 0.5
            when sto.touchpoint_position = 'first' then o.order_total * 0.4
            when sto.touchpoint_position = 'last' then o.order_total * 0.4
            else o.order_total * 0.2 / (sto.total_touchpoints - 2)
        end as attributed_revenue
    from sessions_to_orders as sto
    left join orders as o on sto.order_id = o.order_id
),

-- aggregate by campaign
by_campaign as (
    select
        coalesce(utm_source, 'direct') as utm_source,
        coalesce(utm_medium, 'none') as utm_medium,
        coalesce(utm_campaign, '(not set)') as utm_campaign,
        count(distinct order_id) as orders_touched,
        sum(attribution_weight) as attributed_conversions,
        sum(attributed_revenue) as attributed_revenue,
        -- breakdown by position
        sum(case when touchpoint_position = 'first' then attribution_weight else 0 end) as first_touch_conversions,
        sum(case when touchpoint_position = 'last' then attribution_weight else 0 end) as last_touch_conversions,
        sum(case when touchpoint_position = 'middle' then attribution_weight else 0 end) as middle_touch_conversions
    from with_attribution
    group by 1, 2, 3
)

select
    utm_source,
    utm_medium,
    utm_campaign,
    orders_touched,
    round(attributed_conversions, 2) as attributed_conversions,
    round(attributed_revenue, 2) as attributed_revenue,
    round(first_touch_conversions, 2) as first_touch_conversions,
    round(last_touch_conversions, 2) as last_touch_conversions,
    round(middle_touch_conversions, 2) as middle_touch_conversions
from by_campaign
order by attributed_revenue desc
