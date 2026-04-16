select
    employee_id,
    try_to_date(quit_date) as quit_date
from {{ source('google_drive', 'HR_QUITS')}}