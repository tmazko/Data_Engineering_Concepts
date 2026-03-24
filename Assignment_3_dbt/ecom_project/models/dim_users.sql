select
    user_id, first_name, last_name, city
from {{ ref('stg_users') }}

