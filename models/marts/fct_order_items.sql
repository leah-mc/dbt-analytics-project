with order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_date
    from {{ ref('int_orders_enriched') }}
),

final as (
    select
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        o.order_date,
        oi.product_id,
        oi.product_name,
        oi.category,
        oi.quantity,
        oi.unit_price,
        oi.line_total
    from order_items oi
    left join orders as o on oi.order_id = o.order_id
)

select * from final
