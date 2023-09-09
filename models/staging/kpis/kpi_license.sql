{{ config(
    materialized="table"
)}}

with license_compliance as (
  select
    listings_id,
    case
      when license like '%"Available with a mobility lease only (""bail mobilité"")"%' or license like '%Exempt - hotel-type listing%' then 'Exempt'
      when license like '%null%' then 'Unlicensed'
      when REGEXP_LIKE(license, '\\d+') then 'License'
      when (license not like '%Available%' and license not like '%Exempt%') and not REGEXP_LIKE(license, '\\d+')
      THEN 'License'
      else 'pending'
    end as license_status
  from {{ ref('source_listings_hosts') }}
),


count_license AS (
    SELECT
        license_status,
        count(*) AS count_listings
    FROM license_compliance
    GROUP BY license_status

),
cal_proportion AS (
SELECT
    *,
    ROUND((count_listings / SUM(count_listings) OVER ()) * 100, 1) AS proportion
FROM count_license
)

SELECT 
 *
 FROM cal_proportion
ORDER BY count_listings DESC
