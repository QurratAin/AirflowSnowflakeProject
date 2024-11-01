-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into BRAVE_DATABASE.PROJECT_TEST.fact_campaign_effectiveness_qu as DBT_INTERNAL_DEST
        using BRAVE_DATABASE.PROJECT_TEST.fact_campaign_effectiveness_qu__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.product_id = DBT_INTERNAL_DEST.product_id
                ) and (
                    DBT_INTERNAL_SOURCE.campaign_id = DBT_INTERNAL_DEST.campaign_id
                ) and (
                    DBT_INTERNAL_SOURCE.user_id = DBT_INTERNAL_DEST.user_id
                )

    
    when matched then update set
        "CAMPAIGN_ID" = DBT_INTERNAL_SOURCE."CAMPAIGN_ID","PRODUCT_ID" = DBT_INTERNAL_SOURCE."PRODUCT_ID","PRODUCT_PRICE" = DBT_INTERNAL_SOURCE."PRODUCT_PRICE","TOTAL_UNIT_SOLD" = DBT_INTERNAL_SOURCE."TOTAL_UNIT_SOLD","TOTAL_ATC" = DBT_INTERNAL_SOURCE."TOTAL_ATC","TOTAL_QV" = DBT_INTERNAL_SOURCE."TOTAL_QV","TOTAL_PDP" = DBT_INTERNAL_SOURCE."TOTAL_PDP","USER_ID" = DBT_INTERNAL_SOURCE."USER_ID"
    

    when not matched then insert
        ("CAMPAIGN_ID", "PRODUCT_ID", "PRODUCT_PRICE", "TOTAL_UNIT_SOLD", "TOTAL_ATC", "TOTAL_QV", "TOTAL_PDP", "USER_ID")
    values
        ("CAMPAIGN_ID", "PRODUCT_ID", "PRODUCT_PRICE", "TOTAL_UNIT_SOLD", "TOTAL_ATC", "TOTAL_QV", "TOTAL_PDP", "USER_ID")

;
    commit;