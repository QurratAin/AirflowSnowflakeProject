-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into BRAVE_DATABASE.PROJECT_TEST.dim_search_location_qu as DBT_INTERNAL_DEST
        using BRAVE_DATABASE.PROJECT_TEST.dim_search_location_qu__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.location_id = DBT_INTERNAL_DEST.location_id
            )

    
    when matched then update set
        "LOCATION_ID" = DBT_INTERNAL_SOURCE."LOCATION_ID","COUNTRY" = DBT_INTERNAL_SOURCE."COUNTRY","CITY" = DBT_INTERNAL_SOURCE."CITY","REGION" = DBT_INTERNAL_SOURCE."REGION","ZIPCODE" = DBT_INTERNAL_SOURCE."ZIPCODE"
    

    when not matched then insert
        ("LOCATION_ID", "COUNTRY", "CITY", "REGION", "ZIPCODE")
    values
        ("LOCATION_ID", "COUNTRY", "CITY", "REGION", "ZIPCODE")

;
    commit;