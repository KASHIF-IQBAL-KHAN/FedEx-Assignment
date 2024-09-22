SELECT
    *
FROM 
    {{ ref('src_cleansed_amazon_sales_report') }}
WHERE quantity < 0
LIMIT 10