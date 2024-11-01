-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into BRAVE_DATABASE.PROJECT_TEST.dim_campaign_qu as DBT_INTERNAL_DEST
        using BRAVE_DATABASE.PROJECT_TEST.dim_campaign_qu__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.campaign_id = DBT_INTERNAL_DEST.campaign_id
            )

    
    when matched then update set
        "CAMPAIGN_ID" = DBT_INTERNAL_SOURCE."CAMPAIGN_ID","CAMPAIGN_NAME" = DBT_INTERNAL_SOURCE."CAMPAIGN_NAME","MEDIUM" = DBT_INTERNAL_SOURCE."MEDIUM","SOURCE" = DBT_INTERNAL_SOURCE."SOURCE","CONTENT" = DBT_INTERNAL_SOURCE."CONTENT"
    

    when not matched then insert
        ("CAMPAIGN_ID", "CAMPAIGN_NAME", "MEDIUM", "SOURCE", "CONTENT")
    values
        ("CAMPAIGN_ID", "CAMPAIGN_NAME", "MEDIUM", "SOURCE", "CONTENT")

;
    commit;