select
    host_id,
    host_name
from  {{ source('staging', 'hosts') }}