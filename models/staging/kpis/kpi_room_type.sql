{{ config(
    materialized="table"
)}}


with room_type as (
    select room_type,
    count(*) as nombre 
    from {{ ref('source_listings_hosts') }}
    group by room_type
)

select * from room_type