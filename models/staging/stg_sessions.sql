with source as (
    select * from {{ source('raw', 'sessions') }}
),

renamed as (
    select
        session_id,
        customer_id,
        session_started_at,
        coalesce(utm_source, 'direct') as utm_source,
        coalesce(utm_medium, 'none') as utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        landing_page,
        referrer
    from source
)

select * from renamed
