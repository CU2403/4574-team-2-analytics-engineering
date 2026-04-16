with sessions as (

    select
        client_id,
        session_id,
        try_to_timestamp_ntz(session_at) as session_at_ts,
        ip,
        os
    from {{ ref('base_sessions') }}

),

orders as (

    select
        order_id,
        session_id,
        order_at_ts,
        client_name
    from {{ ref('base_orders') }}

),

session_metrics as (

    select
        client_id,
        min(session_at_ts) as first_session_at,
        max(session_at_ts) as last_session_at,
        count(distinct session_id) as total_sessions
    from sessions
    group by 1

),

latest_session_details as (

    select
        client_id,
        ip,
        os
    from (
        select
            client_id,
            ip,
            os,
            session_id,
            session_at_ts,
            row_number() over (
                partition by client_id
                order by session_at_ts desc, session_id desc
            ) as rn
        from sessions
    )
    where rn = 1

),

order_metrics as (

    select
        s.client_id,
        min(o.order_at_ts) as first_order_at,
        max(o.order_at_ts) as last_order_at,
        count(distinct o.order_id) as total_orders
    from orders as o
    join sessions as s
        on o.session_id = s.session_id
    group by 1

),

client_names as (

    select
        s.client_id,
        max(o.client_name) as client_name
    from orders as o
    join sessions as s
        on o.session_id = s.session_id
    group by 1

),

all_clients as (

    select distinct client_id
    from sessions

),

final as (

    select
        a.client_id,
        n.client_name,
        l.ip,
        l.os,
        sm.first_session_at,
        sm.last_session_at,
        sm.total_sessions,
        om.first_order_at,
        om.last_order_at,
        coalesce(om.total_orders, 0) as total_orders
    from all_clients as a
    left join session_metrics as sm
        on a.client_id = sm.client_id
    left join latest_session_details as l
        on a.client_id = l.client_id
    left join order_metrics as om
        on a.client_id = om.client_id
    left join client_names as n
        on a.client_id = n.client_id

)

select *
from final