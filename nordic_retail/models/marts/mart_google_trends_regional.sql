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
          when 'snus'             then 'Snus'
          when 'nuuska'           then 'Snus'
          when 'nikotinpåsar'     then 'Nicotine Pouches'
          when 'nikotinposer'     then 'Nicotine Pouches'
          when 'nikotiinipussit'  then 'Nicotine Pouches'
          when 'ZYN'              then 'Nicotine Pouches'
      end                         as product_category,

      case keyword
          when 'ZYN' then 'ZYN Brand'
          else 'Category Search'
      end                         as search_type,
      search_interest
  from {{ ref('stg_google_trends_nicotine_by_region') }}
