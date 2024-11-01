

with source as (

   select * from  BRAVE_DATABASE.PROJECT_TEST.stg_user_journey_qu

),

search_data AS (
   SELECT
      
     md5(cast(coalesce(cast(search_model as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(search_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(search_feature as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(search_terms_type as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as search_data_id,
      search_model,  
      search_type,
      search_feature,
      search_terms_type
        
   FROM source
   GROUP BY search_model, 
            search_type, 
            search_feature, 
            search_terms_type
)


SELECT * FROM search_data