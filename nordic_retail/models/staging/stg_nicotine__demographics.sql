 with fhm as (
      select
          country_code,
          cast(year as int64) as year,
          age_group,
          gender,
          product,
          pct_users
      from {{ source('raw_ingest', 'folkhalsomyndigheten_nicotine') }}
  ),

  ssb as (
      select
          country_code,
          year,
          age_group,
          gender,
          product,
          pct_users
      from {{ source('raw_ingest', 'ssb_nicotine_demographics_no') }}
      where year >= 2014
  ),

  combined as (
      select * from fhm
      union all
      select * from ssb
  )

  select * from combined
