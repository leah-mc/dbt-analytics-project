with customers as (
    select * from {{ ref('dim_customers') }}
),

ranked as (
    select
        customer_id,
        customer_name,
        email,
        order_count,
        total_spent,
        first_order,
        last_order
    from customers
    where order_count > 0
)

select * from ranked
order by total_spent desc
