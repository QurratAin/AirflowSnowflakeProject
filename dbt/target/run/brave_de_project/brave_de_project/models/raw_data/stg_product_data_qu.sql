
  create or replace   view BRAVE_DATABASE.PROJECT_TEST.stg_product_data_qu
  
  copy grants as (
    

with source as (

    select * from brave_database.de_project.product_data

),

staging_data as (

    select
        -- Extract product id from varchar string and cast to NUMBER
        
    IFF(
        product_id IS NULL OR TRIM(product_id) = '', 
        0, 
        TRY_CAST(REGEXP_REPLACE(product_id, '[^0-9.]', '') AS NUMBER)
    )
 AS product_id, 
        
        -- Extract warranty_period from varchar to NUMBER
        
    IFF(
        warranty_period IS NULL OR TRIM(warranty_period) = '', 
        0, 
        TRY_CAST(REGEXP_REPLACE(warranty_period, '[^0-9.]', '') AS NUMBER)
    )
 AS warranty_period_months, 
      
        -- Converting to smaller string
        CAST(product_name AS VARCHAR(64)) AS product_name,
        CAST(product_category AS VARCHAR(64)) AS product_category,
        CAST(product_color AS VARCHAR(64)) AS product_color,

        price,
        CAST(supplier_id AS VARCHAR(64)) AS supplier_id,       
        manufacturing_date,
        expiration_date,
        quantity_in_stock,
        rating,
        sales_volume,
        weight_grams,
        discount_percentage

    from source

)

select * from staging_data
  );

