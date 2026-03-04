 with source as (
      select * from {{ source('raw_ingest', 'google_trends_nicotine_weekly') }}
  ),

  cleaned as (
      select
          country_code,
          cast(date as date)          as week_start,
          extract(year from date)     as year,
          extract(month from date)    as month,
          extract(week from date)     as week_of_year,
          keyword,
          search_interest,
          ingested_at
      from source
      where search_interest is not null
  )

  select * from cleaned
  
