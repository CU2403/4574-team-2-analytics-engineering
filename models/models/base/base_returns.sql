select
    order_id,
    returned_at as returned_date,
    is_refunded
from {{ source('google_drive', 'RETURNS') }}