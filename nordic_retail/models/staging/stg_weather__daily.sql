with source as (
    select * from {{ source('raw_ingest', 'weather_daily') }}
),
cleaned as (
    select
        cast(date as date) as date,
        city,
        country_code,
        cast(temperature_2m_mean as float64) as temp_mean,
        cast(temperature_2m_max as float64) as temp_max,
        cast(temperature_2m_min as float64) as temp_min,
        cast(precipitation_sum as float64) as precipitation_mm,
        cast(snowfall_sum as float64) as snowfall_cm,
        cast(wind_speed_10m_max as float64) as wind_speed_max,
        weathercode,
        extract(year from cast(date as date)) as year,
        extract(month from cast(date as date)) as month,
        ingested_at
    from source
    where date is not null
)
select * from cleaned
