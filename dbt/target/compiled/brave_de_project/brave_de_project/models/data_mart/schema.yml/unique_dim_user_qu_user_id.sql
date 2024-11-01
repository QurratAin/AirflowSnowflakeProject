
    
    

select
    user_id as unique_field,
    count(*) as n_records

from BRAVE_DATABASE.PROJECT_TEST.dim_user_qu
where user_id is not null
group by user_id
having count(*) > 1


