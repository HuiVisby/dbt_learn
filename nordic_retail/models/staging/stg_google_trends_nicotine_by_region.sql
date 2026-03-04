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
