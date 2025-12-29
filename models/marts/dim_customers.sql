with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select
        customer_id,
        count(*) as order_count,
        sum(order_total) as total_spent,
        min(order_date) as first_order,
        max(order_date) as last_order
    from {{ ref('int_orders_enriched') }}
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.created_at,
        coalesce(co.order_count, 0) as order_count,
        coalesce(co.total_spent, 0) as total_spent,
        co.first_order,
        co.last_order
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from final
