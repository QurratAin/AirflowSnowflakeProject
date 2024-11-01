
    
    

with all_values as (

    select
        account_status as value_field,
        count(*) as n_records

    from BRAVE_DATABASE.PROJECT_TEST.dim_user_qu
    group by account_status

)

select *
from all_values
where value_field not in (
    'active','in-active','suspended'
)


