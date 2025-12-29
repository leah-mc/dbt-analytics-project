with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        status as order_status,
        created_at as order_date
    from source
)

select * from renamed
