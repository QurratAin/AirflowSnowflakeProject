


with source as (

    select * from brave_database.de_project.inventory_data

),

staging_data as (

    select
        inventory_id,
        warehouse_id,
        stock_level,
        restock_date,
        supplier_id,
        CAST(storage_condition AS VARCHAR(64)) AS storage_condition,
        CAST(inventory_status AS VARCHAR(64)) AS inventory_status,
        reorder_level,
        quantity_in_stock,
        sales_volume,
        safety_stock,
        average_monthly_demand,
        last_restock_date,
        next_restock_date,

        -- Extract product id from varchar string and cast to NUMBER
        
    IFF(
        product_id IS NULL OR TRIM(product_id) = '', 
        0, 
        TRY_CAST(REGEXP_REPLACE(product_id, '[^0-9.]', '') AS NUMBER)
    )
 AS product_id, 
        
        -- Extract last audit date from timestamp and
        -- convert to date format
        CAST(last_audit_date AS DATE) AS last_audit_date, 

        -- Change data type from varchar to NUMBER(38,1)
        
    IFF(
        rating IS NULL OR TRIM(rating) = '', 
        0, 
        TRY_CAST(REPLACE(REGEXP_REPLACE(TRIM(rating), '[^0-9,]', ''), ',', '.') AS NUMBER(38, 1))
    )
 AS rating, 
      
        -- Change data type from varchar to NUMBER(38,1)
        
    IFF(
        weight IS NULL OR TRIM(weight) = '', 
        0, 
        TRY_CAST(REPLACE(REGEXP_REPLACE(TRIM(weight), '[^0-9,]', ''), ',', '.') AS NUMBER(38, 2))
    )
 AS prod_weight, 
        
        -- Change data type to NUMBER(38,2)
        
    IFF(
        discounts IS NULL OR TRIM(discounts) = '', 
        0, 
        TRY_CAST(REPLACE(REGEXP_REPLACE(TRIM(discounts), '[^0-9,]', ''), ',', '.') AS NUMBER(38, 2))
    )
 AS discounts

    from source

)

select * from staging_data