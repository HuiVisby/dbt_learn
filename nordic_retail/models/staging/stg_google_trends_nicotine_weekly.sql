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
  EOF

  cat > ~/dbt_learn/nordic_retail/models/staging/stg_google_trends__nicotine_by_region.sql << 'EOF'
  with source as (
      select * from {{ source('raw_ingest', 'google_trends_nicotine_by_region') }}
  ),

  cleaned as (
      select
          country_code,
          region,
          keyword,
          search_interest,
          ingested_at
      from source
      where search_interest > 0
  )

  select * from cleaned
