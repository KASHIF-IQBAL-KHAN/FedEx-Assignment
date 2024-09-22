SELECT 
    {{ dbt_utils.generate_surrogate_key(['SKU']) }} AS product_id, 
    SKU,
    Style,
    Category,
    Size,
    ASIN
FROM 
    {{ ref('src_cleansed_amazon_sales_report') }}
GROUP BY 
    SKU, Style, Category, Size, ASIN  -- Ensure unique rows for dimensions
