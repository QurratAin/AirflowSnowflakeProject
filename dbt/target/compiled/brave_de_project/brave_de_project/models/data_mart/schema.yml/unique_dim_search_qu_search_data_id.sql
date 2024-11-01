
    
    

select
    search_data_id as unique_field,
    count(*) as n_records

from BRAVE_DATABASE.PROJECT_TEST.dim_search_qu
where search_data_id is not null
group by search_data_id
having count(*) > 1


