
    
    

select
    location_id as unique_field,
    count(*) as n_records

from BRAVE_DATABASE.PROJECT_TEST.dim_search_location_qu
where location_id is not null
group by location_id
having count(*) > 1


