
    
    

select
    product_id as unique_field,
    count(*) as n_records

from BRAVE_DATABASE.PROJECT_TEST.dim_product_qu
where product_id is not null
group by product_id
having count(*) > 1


