-- Testing for the presence of campaign_id in fact_campaign_effectiveness_qu
SELECT
    fce.campaign_id
FROM
    BRAVE_DATABASE.PROJECT_TEST.fact_campaign_effectiveness_qu fce
LEFT JOIN
    BRAVE_DATABASE.PROJECT_TEST.dim_campaign_qu dc
ON
    fce.campaign_id = dc.campaign_id
WHERE
    dc.campaign_id IS NULL