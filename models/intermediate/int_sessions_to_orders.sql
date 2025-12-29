with sessions as (
    select * from {{ ref('stg_sessions') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

-- get all sessions for each customer before their order
sessions_before_order as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        s.session_id,
        s.session_started_at,
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        s.utm_content,
        s.utm_term,
        s.landing_page,
        s.referrer,
        row_number() over (
            partition by o.order_id
            order by s.session_started_at asc
        ) as touchpoint_number,
        count(*) over (partition by o.order_id) as total_touchpoints
    from orders as o
    inner join sessions as s
        on o.customer_id = s.customer_id
        and s.session_started_at <= o.order_date
),

-- identify first and last touchpoints for u-shaped attribution
with_position as (
    select
        *,
        case
            when touchpoint_number = 1 then 'first'
            when touchpoint_number = total_touchpoints then 'last'
            else 'middle'
        end as touchpoint_position
    from sessions_before_order
)

select * from with_position
