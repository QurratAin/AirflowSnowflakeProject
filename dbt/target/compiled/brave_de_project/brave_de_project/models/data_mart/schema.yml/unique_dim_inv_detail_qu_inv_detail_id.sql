
    
    

select
    inv_detail_id as unique_field,
    count(*) as n_records

from BRAVE_DATABASE.PROJECT_TEST.dim_inv_detail_qu
where inv_detail_id is not null
group by inv_detail_id
having count(*) > 1


