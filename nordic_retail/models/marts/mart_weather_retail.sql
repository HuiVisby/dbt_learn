with weather as (
    select
        country_code,
        year,
        month,
        avg(temp_mean) as avg_temp_c,
        sum(precipitation_mm) as total_precipitation_mm,
        sum(snowfall_cm) as total_snowfall_cm,
        avg(wind_speed_max) as avg_wind_speed
    from {{ ref('stg_weather__daily') }}
    where year >= 2014
    group by country_code, year, month
),
retail as (
    select
        country_code,
        year,
        month,
        retail_index
    from {{ ref('mart_retail_index') }}
),
final as (
    select
        w.country_code,
        case w.country_code
            when 'SE' then 'Sweden'
            when 'NO' then 'Norway'
            when 'DK' then 'Denmark'
            when 'FI' then 'Finland'
        end as country_name,
        w.year,
        w.month,
        w.avg_temp_c,
        w.total_precipitation_mm,
        w.total_snowfall_cm,
        w.avg_wind_speed,
        r.retail_index
    from weather w
    left join retail r
        on w.country_code = r.country_code
        and w.year = r.year
        and w.month = r.month
)
select * from final
order by country_code, year, month
