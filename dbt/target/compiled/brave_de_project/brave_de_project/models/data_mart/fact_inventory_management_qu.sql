

WITH inventory_data AS (

    select * from BRAVE_DATABASE.PROJECT_TEST.stg_inventory_data_qu

),

product_data AS (

    select * from BRAVE_DATABASE.PROJECT_TEST.stg_product_data_qu 

),

inv_detail AS (

    select * from BRAVE_DATABASE.PROJECT_TEST.dim_inv_detail_qu
),

inventory_management AS (
    SELECT
    
    id.inventory_id,
    id.product_id,
    id.warehouse_id,
    id.supplier_id,
    idt.inv_detail_id,

    pd.price,
    id.stock_level,
    id.reorder_level,
    id.quantity_in_stock,
    id.sales_volume,
    id.safety_stock,
    id.average_monthly_demand,
    id.rating,
    id.prod_weight,
    id.discounts,
    last_audit_date,
    last_restock_date,
    restock_date,
    next_restock_date
                
    FROM inventory_data id
    INNER JOIN inv_detail idt 
        ON id.inventory_status = idt.storage_condition
        AND idt.inventory_status = id.inventory_status
    LEFT JOIN product_data pd ON id.product_id = pd.product_id
)

SELECT * FROM inventory_management
WHERE price >= discounts