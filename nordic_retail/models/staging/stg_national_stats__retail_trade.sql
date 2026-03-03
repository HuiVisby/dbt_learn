with source as (
    select * from {{ source('raw_ingest', 'national_stats_retail_trade') }}
),
cleaned as (
    select
        country_code,
        replace(period, 'M', '-') as period,
        cast(index_value as float64) as retail_index,
        source as data_source,
        cast(left(replace(period, 'M', '-'), 4) as int64) as year,
        cast(right(replace(period, 'M', '-'), 2) as int64) as month,
        ingested_at
    from source
    where index_value is not null
      and country_code in ('SE', 'NO', 'DK', 'FI')
)
select * from cleaned
