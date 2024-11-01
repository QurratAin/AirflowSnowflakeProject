

with source as (

   select * from BRAVE_DATABASE.PROJECT_TEST.stg_user_journey_qu

),

campaign_data AS (
   SELECT
      --ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS campaign_id,
      md5(cast(coalesce(cast(campaign_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(medium as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(source as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(content as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS campaign_id,
      campaign_name,
      medium,
      source,
      content
     
   FROM source
   GROUP BY campaign_name, medium, source, content
)

SELECT * FROM campaign_data