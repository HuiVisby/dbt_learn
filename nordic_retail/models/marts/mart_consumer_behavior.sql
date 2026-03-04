  with ecommerce as (
      select
          country_code,
          year,
          avg(ecommerce_pct) as ecommerce_pct
      from {{ ref('stg_eurostat__individuals_buying_online') }}
      group by country_code, year
  ),

  confidence as (
      select
          country_code,
          year,
          month,
          period,
          confidence_value
      from {{ ref('stg_eurostat__consumer_confidence') }}
      where year >= 2014
  ),

  final as (
      select
          e.country_code,
          case e.country_code
              when 'SE' then 'Sweden'
              when 'NO' then 'Norway'
              when 'DK' then 'Denmark'
              when 'FI' then 'Finland'
          end                 as country_name,
          e.year,
          c.month,
          c.period,
          c.confidence_value,
          e.ecommerce_pct
      from ecommerce e
      left join confidence c
          on e.country_code = c.country_code
          and e.year        = c.year
  )

  select * from final
  order by country_code, year, month
