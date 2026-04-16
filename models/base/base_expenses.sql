select
    date as expense_date,
    expense_type,
    cast(regexp_replace(expense_amount, '[^0-9.-]', '') as number(10,2)) as expense_amount
from {{ source('google_drive', 'EXPENSES') }}
where nullif(trim(expense_type), '') is not null
  and nullif(trim(expense_amount), '') is not null
