{{
    config(
    materialized = 'incremental',
    on_schema_change = 'fail'
    )
}}


SELECT 
    order_id,  -- Already available unique & not null key for the fact table
    d.date_id,
    p.product_id,
    c.channel_id,
    s.shipping_id,
    o.quantity AS quantity,
    o.Amount AS amount,
    {# o.order_status AS order_status -- will think more on this attribute #}
FROM 
    {{ ref('src_cleansed_amazon_sales_report') }} o
INNER JOIN 
    {{ ref('date_dimension') }} d ON o.order_date = d.date
INNER JOIN 
    {{ ref('product_dimension') }} p ON o.SKU = p.SKU
INNER JOIN 
    {{ ref('sales_channel_dimension') }} c ON o.Sales_Channel = c.sales_channel 
    AND o.Fulfilment = c.fulfilment_type
    AND o.courier_status = c.courier_status
INNER JOIN 
    {{ ref('shipping_dimension') }} s ON o.Ship_City = s.ship_city 
    AND o.Ship_State = s.ship_state 
    AND o.Ship_Postal_Code = s.ship_postal_code
    AND o.Ship_Country = s.ship_country
    AND o.Ship_Service_Level = s.ship_service_level
{% if is_incremental() %}
  {# AND o.order_date > (select max(o.order_date) from {{ this }}) #}
{% endif %}