with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

final as (
    select
        order_id,
        customer_id,
        customer_name,
        order_status,
        order_date,
        order_total,
        item_count
    from orders
)

select * from final
