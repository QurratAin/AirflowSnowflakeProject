-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into BRAVE_DATABASE.PROJECT_TEST.dim_inv_detail_qu as DBT_INTERNAL_DEST
        using BRAVE_DATABASE.PROJECT_TEST.dim_inv_detail_qu__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.inv_detail_id = DBT_INTERNAL_DEST.inv_detail_id
            )

    
    when matched then update set
        "INV_DETAIL_ID" = DBT_INTERNAL_SOURCE."INV_DETAIL_ID","STORAGE_CONDITION" = DBT_INTERNAL_SOURCE."STORAGE_CONDITION","INVENTORY_STATUS" = DBT_INTERNAL_SOURCE."INVENTORY_STATUS"
    

    when not matched then insert
        ("INV_DETAIL_ID", "STORAGE_CONDITION", "INVENTORY_STATUS")
    values
        ("INV_DETAIL_ID", "STORAGE_CONDITION", "INVENTORY_STATUS")

;
    commit;