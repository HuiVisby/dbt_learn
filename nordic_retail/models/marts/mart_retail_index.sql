with eurostat as (
    select
        country_code,
        period,
        year,
        month,
        retail_index,
        'Eurostat' as data_source
    from {{ ref('stg_eurostat__retail_trade') }}
),
national as (
    select
        country_code,
        period,
        year,
        month,
        retail_index,
        data_source
    from {{ ref('stg_national_stats__retail_trade') }}
    where year >= 2014
),
combined as (
    select * from national
    union all
    select e.* from eurostat e
    where not exists (
        select 1 from national n
        where n.country_code = e.country_code
          and n.period = e.period
    )
),
final as (
    select
        country_code,
        case country_code
            when 'SE' then 'Sweden'
            when 'NO' then 'Norway'
            when 'DK' then 'Denmark'
            when 'FI' then 'Finland'
        end as country_name,
        period,
        year,
        month,
        retail_index,
        data_source
    from combined
    where year >= 2014
)
select * from final
order by country_code, period
