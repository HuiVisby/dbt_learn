  {{
      config(materialized='table')
  }}

  select
      country_code,
      case country_code
          when 'SE' then 'Sweden'
          when 'NO' then 'Norway'
          when 'DK' then 'Denmark'
          when 'FI' then 'Finland'
      end                         as country_name,
      region,
      keyword,
      case keyword
          when 'snus'   then 'Snus'
          when 'nuuska' then 'Snus'
      end                         as product_category,
      search_interest
  from {{ ref('stg_google_trends_nicotine_by_region') }}
