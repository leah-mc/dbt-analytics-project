-- u-shaped attribution model
-- assigns 40% credit to first touch, 40% to last touch, and 20% distributed among middle touches

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
        sto.touchpoint_number,
        sto.total_touchpoints,
        o.order_total,
        case
            -- single touchpoint gets 100%
            when sto.total_touchpoints = 1 then 1.0
            -- two touchpoints: 50% each (first is also last)
            when sto.total_touchpoints = 2 then 0.5
            -- first touch gets 40%
            when sto.touchpoint_position = 'first' then 0.4
            -- last touch gets 40%
            when sto.touchpoint_position = 'last' then 0.4
            -- middle touches share remaining 20%
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

-- aggregate by source/medium
by_source_medium as (
    select
        utm_source,
        utm_medium,
        count(distinct order_id) as orders_touched,
        sum(attribution_weight) as attributed_conversions,
        sum(attributed_revenue) as attributed_revenue,
        round(avg(attribution_weight), 4) as avg_attribution_weight
    from with_attribution
    group by utm_source, utm_medium
),

-- aggregate by campaign
by_campaign as (
    select
        utm_source,
        utm_medium,
        utm_campaign,
        count(distinct order_id) as orders_touched,
        sum(attribution_weight) as attributed_conversions,
        sum(attributed_revenue) as attributed_revenue
    from with_attribution
    where utm_campaign is not null
    group by utm_source, utm_medium, utm_campaign
)

select
    utm_source,
    utm_medium,
    orders_touched,
    round(attributed_conversions, 2) as attributed_conversions,
    round(attributed_revenue, 2) as attributed_revenue,
    avg_attribution_weight
from by_source_medium
order by attributed_revenue desc
