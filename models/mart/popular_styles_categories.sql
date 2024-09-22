SELECT 
    pd.style,
    pd.category,
    SUM(o.quantity) AS total_quantity
FROM 
    {{ ref('orders_fact') }} o
JOIN 
    {{ ref('product_dimension') }} pd ON o.product_id = pd.product_id
JOIN 
    {{ ref('shipping_dimension') }} sd ON o.shipping_id = sd.shipping_id
WHERE 
    sd.ship_city = 'MUMBAI'
GROUP BY 
    pd.style, pd.category
ORDER BY 
    total_quantity DESC
LIMIT 10
