select
    j.employee_id,
    j.hire_date,
    q.quit_date,
    j.city,
    j.annual_salary,
    j.title
from {{ source('google_drive', 'HR_JOINS') }} as j
left join {{ source('google_drive', 'HR_QUITS') }} as q
    on j.employee_id = q.employee_id