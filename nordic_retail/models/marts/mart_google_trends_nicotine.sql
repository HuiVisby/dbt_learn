  select
      country_code,
      case country_code
          when 'SE' then 'Sweden'
          when 'NO' then 'Norway'
          when 'DK' then 'Denmark'
          when 'FI' then 'Finland'
      end                         as country_name,
      week_start,
      year,
      month,
      week_of_year,
      keyword,
      case keyword
          when 'snus'             then 'Snus'
          when 'nikotinpåsar'     then 'Nicotine Pouches'
          when 'nikotinposer'     then 'Nicotine Pouches'
          when 'nikotiinipussit'  then 'Nicotine Pouches'
          when 'nuuska'           then 'Snus'
          when 'ZYN'              then 'ZYN'
      end                         as product_category,
      search_interest
  from {{ ref('stg_google_trends_nicotine_weekly') }}
