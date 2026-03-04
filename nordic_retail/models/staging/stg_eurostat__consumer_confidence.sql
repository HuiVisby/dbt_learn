  with source as (
      select * from {{ source('raw_ingest', 'eurostat_consumer_confidence') }}
  ),

  cleaned as (
      select
          country_code,
          period,
          pct_value                        as confidence_value,
          cast(left(period, 4) as int64)   as year,
          cast(right(period, 2) as int64)  as month,
          ingested_at
      from source
      where country_code in ('SE', 'NO', 'DK', 'FI')
        and pct_value is not null
  )

  select * from cleaned
