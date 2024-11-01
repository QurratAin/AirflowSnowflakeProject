
    
    

select
    campaign_id as unique_field,
    count(*) as n_records

from BRAVE_DATABASE.PROJECT_TEST.dim_campaign_qu
where campaign_id is not null
group by campaign_id
having count(*) > 1


