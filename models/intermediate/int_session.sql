with sessions as (

    select
        session_id,
        client_id,
        try_to_timestamp_ntz(session_at) as session_at_ts,
        ip,
        os
    from {{ ref('base_sessions') }}

),

page_view_metrics as (

    select
        session_id,
        count(*) as total_page_views,
        count(distinct page_name) as unique_pages_viewed
    from {{ ref('base_page_views') }}
    group by 1

),

item_view_metrics as (

    select
        session_id,
        count(*) as total_item_views,
        count(distinct item_name) as unique_items_viewed,
        sum(coalesce(add_to_cart_quantity, 0)) as total_add_to_cart_quantity,
        sum(coalesce(remove_from_cart_quantity, 0)) as total_remove_from_cart_quantity,
        count_if(coalesce(add_to_cart_quantity, 0) > 0) as add_to_cart_events,
        count_if(coalesce(remove_from_cart_quantity, 0) > 0) as remove_from_cart_events,
        min(item_view_at_ts) as first_item_view_at,
        max(item_view_at_ts) as last_item_view_at
    from {{ ref('base_item_view') }}
    group by 1

),

order_metrics as (

    select
        session_id,
        count(distinct order_id) as total_orders,
        min(order_at_ts) as first_order_at,
        max(order_at_ts) as last_order_at,
        sum(coalesce(shipping_cost, 0)) as total_shipping_cost
    from {{ ref('base_orders') }}
    group by 1

),

final as (

    select
        s.session_id,
        s.client_id,
        s.session_at_ts,
        s.ip,
        s.os,

        coalesce(p.total_page_views, 0) as total_page_views,
        coalesce(p.unique_pages_viewed, 0) as unique_pages_viewed,

        coalesce(i.total_item_views, 0) as total_item_views,
        coalesce(i.unique_items_viewed, 0) as unique_items_viewed,
        coalesce(i.total_add_to_cart_quantity, 0) as total_add_to_cart_quantity,
        coalesce(i.total_remove_from_cart_quantity, 0) as total_remove_from_cart_quantity,
        coalesce(i.add_to_cart_events, 0) as add_to_cart_events,
        coalesce(i.remove_from_cart_events, 0) as remove_from_cart_events,
        i.first_item_view_at,
        i.last_item_view_at,

        coalesce(o.total_orders, 0) as total_orders,
        o.first_order_at,
        o.last_order_at,
        coalesce(o.total_shipping_cost, 0) as total_shipping_cost,

        case
            when coalesce(p.total_page_views, 0) > 0 then 1
            else 0
        end as has_page_view,

        case
            when coalesce(i.total_item_views, 0) > 0 then 1
            else 0
        end as has_item_view,

        case
            when coalesce(i.add_to_cart_events, 0) > 0 then 1
            else 0
        end as has_add_to_cart,

        case
            when coalesce(o.total_orders, 0) > 0 then 1
            else 0
        end as has_order

    from sessions as s
    left join page_view_metrics as p
        on s.session_id = p.session_id
    left join item_view_metrics as i
        on s.session_id = i.session_id
    left join order_metrics as o
        on s.session_id = o.session_id

)

select *
from final