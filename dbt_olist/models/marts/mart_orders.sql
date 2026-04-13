with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

items as (
    select
        order_id,
        count(*)                as total_items,
        sum(price)              as items_revenue,
        sum(freight_value)      as freight_revenue,
        sum(price + freight_value) as total_revenue
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value)      as payment_total,
        max(payment_installments) as max_installments,
        count(distinct payment_type) as payment_methods
    from {{ ref('stg_order_payments') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.city                          as customer_city,
        c.state                         as customer_state,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.delivered_customer_at,
        o.estimated_delivery_at,

        -- Tiempos
        datediff('day', o.purchased_at, o.approved_at)
            as days_to_approve,
        datediff('day', o.purchased_at, o.delivered_customer_at)
            as days_to_deliver,
        datediff('day', o.delivered_customer_at, o.estimated_delivery_at)
            as days_early_late,

        -- Financiero
        coalesce(i.total_items, 0)      as total_items,
        coalesce(i.items_revenue, 0)    as items_revenue,
        coalesce(i.freight_revenue, 0)  as freight_revenue,
        coalesce(i.total_revenue, 0)    as total_revenue,
        coalesce(p.payment_total, 0)    as payment_total,
        coalesce(p.max_installments, 0) as max_installments,
        coalesce(p.payment_methods, 0)  as payment_methods,

        -- Fechas
        date_trunc('month', o.purchased_at) as order_month,
        date_trunc('year', o.purchased_at)  as order_year

    from orders o
    left join customers c   on o.customer_id = c.customer_id
    left join items i       on o.order_id = i.order_id
    left join payments p    on o.order_id = p.order_id
)

select * from final