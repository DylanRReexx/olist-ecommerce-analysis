with products as (
    select * from {{ ref('stg_products') }}
),

items as (
    select
        product_id,
        count(distinct order_id)    as total_orders,
        sum(price)                  as total_revenue,
        avg(price)                  as avg_price,
        sum(freight_value)          as total_freight
    from {{ ref('stg_order_items') }}
    group by product_id
),

final as (
    select
        p.product_id,
        p.category,
        p.weight_g,
        p.photos_qty,
        coalesce(i.total_orders, 0)     as total_orders,
        coalesce(i.total_revenue, 0)    as total_revenue,
        coalesce(i.avg_price, 0)        as avg_price,
        coalesce(i.total_freight, 0)    as total_freight
    from products p
    left join items i on p.product_id = i.product_id
)

select * from final