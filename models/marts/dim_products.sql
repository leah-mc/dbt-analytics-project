with products as (
    select * from {{ ref('stg_products') }}
),

product_sales as (
    select
        product_id,
        sum(quantity) as units_sold,
        sum(line_total) as revenue
    from {{ ref('int_order_items_enriched') }}
    group by product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        coalesce(ps.units_sold, 0) as units_sold,
        coalesce(ps.revenue, 0) as revenue
    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from final
