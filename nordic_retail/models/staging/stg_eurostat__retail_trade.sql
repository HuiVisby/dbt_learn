with source as (
      select * from {{ source('raw_ingest', 'eurostat_retail_trade') }}
  ),

  cleaned as (
      select
          country_code,
          period,
          nace_r2                          as industry_code,
          s_adj                            as seasonal_adjustment,
          unit                             as unit_measure,
          cast(index_value as float64)     as retail_index,
          cast(left(period, 4) as int64)   as year,
          cast(right(period, 2) as int64)  as month,
          ingested_at
      from source
      where country_code in ('SE', 'NO', 'DK', 'FI')
        and index_value is not null
  )

  select * from cleaned
