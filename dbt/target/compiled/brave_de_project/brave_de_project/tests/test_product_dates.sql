-- Test: Test that the manufacturing date is before the expiration date

SELECT
    COUNT(*) AS invalid_rows
FROM
    BRAVE_DATABASE.PROJECT_TEST.dim_product_qu
WHERE
    manufacturing_date > expiration_date
HAVING
    invalid_rows > 0