with orders_daily as (

    select
        cast(order_at_ts as date) as finance_date,
        sum(coalesce(gross_item_revenue, 0)) as gross_revenue,
        sum(
            case
                when is_refunded_flag = 1 then coalesce(gross_item_revenue, 0)
                else 0
            end
        ) as refunded_revenue,
        sum(coalesce(net_item_revenue, 0)) as net_revenue,
        sum(coalesce(shipping_cost, 0)) as shipping_cost
    from {{ ref('int_order') }}
    group by 1

),

expenses_daily as (

    select
        expense_date as finance_date,

        sum(
            case
                when lower(trim(expense_type)) = 'tech tool' then coalesce(expense_amount, 0)
                else 0
            end
        ) as tech_tool_expense,

        sum(
            case
                when lower(trim(expense_type)) = 'warehouse' then coalesce(expense_amount, 0)
                else 0
            end
        ) as warehouse_expense,

        sum(
            case
                when lower(trim(expense_type)) = 'other' then coalesce(expense_amount, 0)
                else 0
            end
        ) as other_expense,

        sum(
            case
                when lower(trim(expense_type)) = 'hr' then coalesce(expense_amount, 0)
                else 0
            end
        ) as raw_hr_expense

    from {{ ref('base_expenses') }}
    group by 1

),

employee_dates as (

    select
        employee_id,
        annual_salary,
        try_to_date(hire_date) as hire_date,
        quit_date
    from {{ ref('int_employee') }}
    where try_to_date(hire_date) is not null

),

source_dates as (

    select cast(order_at_ts as date) as dt
    from {{ ref('int_order') }}

    union all

    select expense_date as dt
    from {{ ref('base_expenses') }}

    union all

    select try_to_date(hire_date) as dt
    from {{ ref('int_employee') }}
    where try_to_date(hire_date) is not null

    union all

    select quit_date as dt
    from {{ ref('int_employee') }}
    where quit_date is not null
      and quit_date <= current_date()

),

date_bounds as (

    select
        min(dt) as min_date,
        least(max(dt), current_date()) as max_date
    from source_dates

),

numbers as (

    select seq4() as n
    from table(generator(rowcount => 10000))

),

date_spine as (

    select
        dateadd(day, n, b.min_date) as finance_date
    from numbers
    cross join date_bounds b
    where dateadd(day, n, b.min_date) <= b.max_date

),

payroll_daily as (

    select
        d.finance_date,
        count(distinct e.employee_id) as active_employee_count,
        sum(coalesce(e.annual_salary, 0) / 365.0) as estimated_payroll_expense
    from date_spine d
    join employee_dates e
        on d.finance_date >= e.hire_date
       and (e.quit_date is null or d.finance_date <= e.quit_date)
    group by 1

),

final as (

    select
        d.finance_date,

        coalesce(o.gross_revenue, 0) as gross_revenue,
        coalesce(o.refunded_revenue, 0) as refunded_revenue,
        coalesce(o.net_revenue, 0) as net_revenue,
        coalesce(o.shipping_cost, 0) as shipping_cost,

        coalesce(e.tech_tool_expense, 0) as tech_tool_expense,
        coalesce(e.warehouse_expense, 0) as warehouse_expense,
        coalesce(e.other_expense, 0) as other_expense,
        coalesce(e.raw_hr_expense, 0) as raw_hr_expense,

        coalesce(p.active_employee_count, 0) as active_employee_count,
        coalesce(p.estimated_payroll_expense, 0) as estimated_payroll_expense,

        coalesce(o.shipping_cost, 0)
        + coalesce(e.tech_tool_expense, 0)
        + coalesce(e.warehouse_expense, 0)
        + coalesce(e.other_expense, 0)
        + coalesce(e.raw_hr_expense, 0) as total_costs_booked,

        coalesce(o.shipping_cost, 0)
        + coalesce(e.tech_tool_expense, 0)
        + coalesce(e.warehouse_expense, 0)
        + coalesce(e.other_expense, 0)
        + coalesce(p.estimated_payroll_expense, 0) as total_costs_estimated,

        coalesce(o.net_revenue, 0)
        - (
            coalesce(o.shipping_cost, 0)
            + coalesce(e.tech_tool_expense, 0)
            + coalesce(e.warehouse_expense, 0)
            + coalesce(e.other_expense, 0)
            + coalesce(e.raw_hr_expense, 0)
        ) as booked_profit,

        coalesce(o.net_revenue, 0)
        - (
            coalesce(o.shipping_cost, 0)
            + coalesce(e.tech_tool_expense, 0)
            + coalesce(e.warehouse_expense, 0)
            + coalesce(e.other_expense, 0)
            + coalesce(p.estimated_payroll_expense, 0)
        ) as estimated_profit

    from date_spine d
    left join orders_daily o
        on d.finance_date = o.finance_date
    left join expenses_daily e
        on d.finance_date = e.finance_date
    left join payroll_daily p
        on d.finance_date = p.finance_date

)

select *
from final
order by finance_date