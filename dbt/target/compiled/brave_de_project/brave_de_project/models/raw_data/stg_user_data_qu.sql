

with source as (

    select * from brave_database.de_project.user_data

),

staging_data as (

    select
        user_id,
        
        -- Converting to short field
        CAST(first_name AS VARCHAR(64)) AS first_name,
        CAST(last_name AS VARCHAR(64)) AS last_name,
        CAST(preferred_language AS VARCHAR(64)) AS preferred_language,
        CAST(account_status AS VARCHAR(64)) AS account_status,

        -- Checking for invalid email id format 
        CASE 
            WHEN email not like '%_@__%.__%' THEN NULL 
            ELSE CAST(email AS VARCHAR(256)) 
        END AS email,

        -- Convert signup_date from VARCHAR to DATE
        
    IFF(
        signup_date IS NULL OR TRIM(signup_date) = '', 
        NULL, 
        IFF(
            REGEXP_LIKE(TRIM(signup_date), '^\d{4}-\d{2}-\d{2}$'), 
            TRY_CAST(TRIM(signup_date) AS DATE), 
            NULL
        )
    )
 AS signup_date,

        -- Convert dob from VARCHAR to DATE
        
    IFF(
        dob IS NULL OR TRIM(dob) = '', 
        NULL, 
        IFF(
            REGEXP_LIKE(TRIM(dob), '^\d{4}-\d{2}-\d{2}$'), 
            TRY_CAST(TRIM(dob) AS DATE), 
            NULL
        )
    )
 AS dob,

         -- Convert marketing_opt_in from VARCHAR to BOOLEAN
        CASE 
            WHEN marketing_opt_in = 'True' THEN TRUE
            WHEN marketing_opt_in = 'False' THEN FALSE
            ELSE False 
        END AS marketing_opt_in,

        -- Convert loyalty_points_balance from VARCHAR to NUMBER
        
    IFF(
        loyalty_points_balance IS NULL OR TRIM(loyalty_points_balance) = '', 
        0, 
        TRY_CAST(REGEXP_REPLACE(loyalty_points_balance, '[^0-9.]', '') AS NUMBER)
    )
 AS loyalty_points_balance

    from source

)

select * from staging_data