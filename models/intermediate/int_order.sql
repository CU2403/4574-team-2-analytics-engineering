with orders as (

    select
        order_id,
        session_id,
        order_at_ts,
        client_name,
        payment_info,
        payment_method,
        phone,
        shipping_address,
        shipping_cost,
        state,
        tax_rate
    from {{ ref('base_orders') }}

),

sessions as (

    select
        session_id,
        client_id
    from {{ ref('base_sessions') }}

),

returns as (

    select
        order_id,
        returned_date,
        is_refunded
    from {{ ref('base_returns') }}

),

item_metrics as (

    select
        session_id,
        count(distinct item_name) as unique_items_ordered,
        sum(coalesce(add_to_cart_quantity, 0)) as total_units_ordered,
        avg(price_per_unit) as avg_price_per_unit,
        sum(coalesce(add_to_cart_quantity, 0) * coalesce(price_per_unit, 0)) as gross_item_revenue
    from {{ ref('base_item_view') }}
    where coalesce(add_to_cart_quantity, 0) > 0
    group by 1

),

final as (

    select
        o.order_id,
        o.session_id,
        s.client_id,
        o.order_at_ts,
        o.client_name,
        o.payment_info,
        o.payment_method,
        o.phone,
        o.shipping_address,
        o.shipping_cost,
        o.state,
        o.tax_rate,

        coalesce(i.unique_items_ordered, 0) as unique_items_ordered,
        coalesce(i.total_units_ordered, 0) as total_units_ordered,
        i.avg_price_per_unit,
        coalesce(i.gross_item_revenue, 0) as gross_item_revenue,

        r.returned_date,
        r.is_refunded,

        case
            when lower(coalesce(r.is_refunded, 'no')) = 'yes' then 1
            else 0
        end as is_refunded_flag,

        case
            when lower(coalesce(r.is_refunded, 'no')) = 'yes' then 0
            else coalesce(i.gross_item_revenue, 0)
        end as net_item_revenue

    from orders as o
    left join sessions as s
        on o.session_id = s.session_id
    left join item_metrics as i
        on o.session_id = i.session_id
    left join returns as r
        on o.order_id = r.order_id

)

select *
from final