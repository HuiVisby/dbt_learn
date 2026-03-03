  with src as (
      select * from {{ ref('stg_nicotine__demographics') }}
  )

  select
      country_code,
      case country_code
          when 'SE' then 'Sweden'
          when 'NO' then 'Norway'
      end                                         as country_name,
      year,
      age_group,
      gender,
      product,
      pct_users,
      year = max(year) over (
          partition by country_code, product
      )                                           as is_latest_year
  from src
