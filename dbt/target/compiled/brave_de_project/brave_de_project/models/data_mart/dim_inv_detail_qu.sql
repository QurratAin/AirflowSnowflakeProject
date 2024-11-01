

with source as (

   select * from BRAVE_DATABASE.PROJECT_TEST.stg_inventory_data_qu
),


inventory_data AS (
   SELECT
      md5(cast(coalesce(cast(storage_condition as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(inventory_status as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as inv_detail_id,
      storage_condition,
      inventory_status
      
   FROM source
   GROUP BY storage_condition, inventory_status
)


SELECT * FROM inventory_data