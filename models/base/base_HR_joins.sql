select
    employee_id,
    try_to_date(regexp_substr(hire_date, '\\d{4}-\\d{2}-\\d{2}')) as hire_date,
    city,
    annual_salary,
    title
from {{ source('google_drive', 'HR_JOINS')}}