
    
    

with all_values as (

    select
        inventory_status as value_field,
        count(*) as n_records

    from BRAVE_DATABASE.PROJECT_TEST.dim_inv_detail_qu
    group by inventory_status

)

select *
from all_values
where value_field not in (
    'in-stock','backordered'
)


