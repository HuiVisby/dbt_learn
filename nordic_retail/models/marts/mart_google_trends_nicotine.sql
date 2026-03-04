  with weekly as (
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
          -- Normalize keyword to English for cross-country comparison
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
  ),

  regional as (
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
          search_interest             as regional_interest
      from {{ ref('stg_google_trends_nicotine_by_region') }}
  )

  select
      w.*,
      r.region,
      r.regional_interest
  from weekly w
  left join regional r
      on w.country_code = r.country_code
      and w.keyword = r.keyword
