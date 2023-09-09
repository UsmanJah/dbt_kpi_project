{{ config(
    materialized="table"
)}}


with count_paris as (
    select count(listings_id) as listings
    from {{ ref('source_listings_hosts') }}
  
)

select * from count_paris