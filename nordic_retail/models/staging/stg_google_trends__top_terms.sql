with source as (
    select * from {{ source('raw_ingest', 'google_trends_top_terms') }}
),
cleaned as (
    select
        cast(week as date) as week,
        country_code,
        lower(term) as term,
        rank,
        score,
        extract(year from cast(week as date)) as year,
        extract(month from cast(week as date)) as month,
        ingested_at
    from source
    where country_code in ('SE', 'NO', 'DK', 'FI')
      and week is not null
)
select * from cleaned
