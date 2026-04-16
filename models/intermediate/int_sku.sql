with item_orders as (

    select
        o.order_id,
        o.session_id,
        o.order_at_ts,
        iv.item_name,
        iv.price_per_unit,
        coalesce(iv.add_to_cart_quantity, 0) as units_sold
    from {{ ref('base_orders') }} as o
    join {{ ref('base_item_view') }} as iv
        on o.session_id = iv.session_id
    where coalesce(iv.add_to_cart_quantity, 0) > 0

),

item_orders_with_returns as (

    select
        io.order_id,
        io.session_id,
        io.order_at_ts,
        io.item_name,
        io.price_per_unit,
        io.units_sold,
        r.returned_date,
        r.is_refunded
    from item_orders as io
    left join {{ ref('base_returns') }} as r
        on io.order_id = r.order_id

)

select
    item_name,
    sum(units_sold) as gross_units_sold,
    sum(
        case
            when lower(coalesce(is_refunded, 'false')) = 'true' then units_sold
            else 0
        end
    ) as returned_units,
    sum(
        case
            when lower(coalesce(is_refunded, 'false')) = 'true' then 0
            else units_sold
        end
    ) as net_units_sold,
    avg(price_per_unit) as avg_price_per_unit,
    sum(
        case
            when lower(coalesce(is_refunded, 'false')) = 'true' then 0
            else units_sold * price_per_unit
        end
    ) as net_revenue
from item_orders_with_returns
group by 1