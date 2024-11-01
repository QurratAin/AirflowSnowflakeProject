

with source as (

   select * from  BRAVE_DATABASE.PROJECT_TEST.stg_user_journey_qu

),

search_location_data AS (
   SELECT
       
      md5(cast(coalesce(cast(country as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(city as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(region as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(zipcode as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_id, 
      country,
      city,
      region,
      zipcode

   FROM source
   GROUP BY city, country, region, zipcode
)

select * from search_location_data