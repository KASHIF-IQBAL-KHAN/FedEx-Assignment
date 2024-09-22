SELECT 
    {{ dbt_utils.generate_surrogate_key(['Ship_City', 'Ship_State', 'Ship_Postal_Code', 'Ship_Country']) }} AS shipping_id, 
    Ship_Service_Level as ship_service_level,
    Ship_City AS ship_city,
    Ship_State AS ship_state,
    Ship_Postal_Code AS ship_postal_code,
    Ship_Country AS ship_country
FROM 
    {{ ref('src_cleansed_amazon_sales_report') }}
GROUP BY 
    Ship_Service_Level, Ship_City, Ship_State, Ship_Postal_Code, Ship_Country  -- Ensure unique rows for dimensions
