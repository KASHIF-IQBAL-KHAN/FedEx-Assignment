SELECT 
    {{ dbt_utils.generate_surrogate_key(['order_date']) }} as date_id, 
    order_date AS date,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    EXTRACT(DAY FROM order_date) AS day,
    EXTRACT(QUARTER FROM order_date) AS quarter
FROM  {{ ref('src_cleansed_amazon_sales_report') }}
GROUP BY order_date  -- Ensure unique rows for dimensions
