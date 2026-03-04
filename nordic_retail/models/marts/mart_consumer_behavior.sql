  with confidence as (
      select
          country_code,
          year,
          month,
          period,
          confidence_value
      from {{ ref('stg_eurostat__consumer_confidence') }}
      where year >= 2014
  ),

  ecommerce as (
      select
          country_code,
          year,
          avg(ecommerce_pct) as ecommerce_pct
      from {{ ref('stg_eurostat__individuals_buying_online') }}
      group by country_code, year
  ),

  final as (
      select
          c.country_code,
          case c.country_code
              when 'SE' then 'Sweden'
              when 'NO' then 'Norway'
              when 'DK' then 'Denmark'
              when 'FI' then 'Finland'
          end                 as country_name,
          c.year,
          c.month,
          c.period,
          c.confidence_value,
          e.ecommerce_pct
      from confidence c
      left join ecommerce e
          on c.country_code = e.country_code
          and c.year        = e.year
  )

  select * from final
  order by country_code, year, month
