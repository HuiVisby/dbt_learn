with source as (
    select * from {{ source('raw_ingest', 'eurostat_online_purchase_frequency') }}
),
cleaned as (
    select
        country_code,
        period,
        cast(pct_value as float64) as pct_value,
        cast(left(period, 4) as int64) as year,
        ingested_at
    from source
    where country_code in ('SE', 'NO', 'DK', 'FI')
      and pct_value is not null
)
select * from cleaned
