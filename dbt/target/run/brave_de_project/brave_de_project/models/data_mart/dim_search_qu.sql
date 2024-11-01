-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into BRAVE_DATABASE.PROJECT_TEST.dim_search_qu as DBT_INTERNAL_DEST
        using BRAVE_DATABASE.PROJECT_TEST.dim_search_qu__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.search_data_id = DBT_INTERNAL_DEST.search_data_id
            )

    
    when matched then update set
        "SEARCH_DATA_ID" = DBT_INTERNAL_SOURCE."SEARCH_DATA_ID","SEARCH_MODEL" = DBT_INTERNAL_SOURCE."SEARCH_MODEL","SEARCH_TYPE" = DBT_INTERNAL_SOURCE."SEARCH_TYPE","SEARCH_FEATURE" = DBT_INTERNAL_SOURCE."SEARCH_FEATURE","SEARCH_TERMS_TYPE" = DBT_INTERNAL_SOURCE."SEARCH_TERMS_TYPE"
    

    when not matched then insert
        ("SEARCH_DATA_ID", "SEARCH_MODEL", "SEARCH_TYPE", "SEARCH_FEATURE", "SEARCH_TERMS_TYPE")
    values
        ("SEARCH_DATA_ID", "SEARCH_MODEL", "SEARCH_TYPE", "SEARCH_FEATURE", "SEARCH_TERMS_TYPE")

;
    commit;