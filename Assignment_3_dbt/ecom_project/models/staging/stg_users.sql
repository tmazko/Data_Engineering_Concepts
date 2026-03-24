select
    user_id, first_name, last_name, email, city,
    cast(registered_at as timestamp) as registered_at -- !!!! is it necessary to set it by hand??
from {{ ref('raw_users') }}

