SELECT 
    {{ dbt_utils.generate_surrogate_key(['Sales_Channel', 'Fulfilment']) }} AS channel_id, 
    Sales_Channel,
    Fulfilment AS fulfilment_type,
    fulfilled_by,
    courier_status
FROM 
    {{ ref('src_cleansed_amazon_sales_report') }}
GROUP BY 
    Sales_Channel, Fulfilment, fulfilled_by, courier_status  -- Ensure unique rows for dimensions
