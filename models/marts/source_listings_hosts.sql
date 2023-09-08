{{ config (
    materialized="table"
)}}


with listings as (
  select * from  {{  ref('stg_hosts')}}
),

 hosts as (
  select * from {{ ref('stg_listings')}}
 ),
final as (
    select  *
    from listings
    left join hosts using(host_id)
)

select * from final