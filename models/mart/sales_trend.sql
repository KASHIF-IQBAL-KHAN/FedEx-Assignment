SELECT 
    d.month,
    d.year,
    sum(o.amount) as total_amount
FROM 
    {{ ref('orders_fact') }} o
INNER JOIN 
    {{ ref('date_dimension') }} d ON o.date_id = d.date_id
GROUP BY 
    d.month,
    d.year
ORDER BY 
    d.month,
    d.year

