

with source as (

   select * from BRAVE_DATABASE.PROJECT_TEST.stg_product_data_qu
),

product_data AS (
   SELECT
   
       product_id,
       product_name,
       product_category,
       product_color,
       supplier_id,
       manufacturing_date,
       expiration_date,
       warranty_period_months
   
   FROM source
)


SELECT * FROM product_data