  with retail as (
      select * from {{ ref('stg_eurostat__retail_trade') }}
  ),

  confidence as (
      select * from {{ ref('stg_eurostat__consumer_confidence') }}
  ),

  joined as (
      select
          r.country_code,
          r.year,
          r.month,
          r.period,
          r.retail_index,
          r.industry_code,
          c.confidence_value,
          case r.country_code
              when 'SE' then 'Sweden'
              when 'NO' then 'Norway'
              when 'DK' then 'Denmark'
              when 'FI' then 'Finland'
          end as country_name
      from retail r
      left join confidence c
          on r.country_code = c.country_code
          and r.period = c.period
      where r.year >= 2014
  )

  select * from joined
