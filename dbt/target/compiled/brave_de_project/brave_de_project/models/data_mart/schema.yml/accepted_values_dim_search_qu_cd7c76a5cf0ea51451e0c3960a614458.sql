
    
    

with all_values as (

    select
        search_terms_type as value_field,
        count(*) as n_records

    from BRAVE_DATABASE.PROJECT_TEST.dim_search_qu
    group by search_terms_type

)

select *
from all_values
where value_field not in (
    'manual-search','type-ahead'
)


