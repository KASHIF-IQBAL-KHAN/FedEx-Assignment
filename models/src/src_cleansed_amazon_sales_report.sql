{# Performing "Data Cleaning" While Capturing Data From Raw Layer to Staging Layer #}

WITH cleansed_amazon_sales_report AS(
SELECT
  *,
  row_number() over (partition by orderid order by date desc) as rnk
FROM
  {{ ref('src_raw_amazon_sales_report') }}
)

SELECT
    trim(orderid) as order_id, 
    to_date(date, 'MM-DD-YY') as order_date, 
    coalesce(lower(trim(status)), 'N/A') as order_status, 
    trim(fulfilment) as fulfilment, 
    saleschannel as sales_channel, 
    shipservicelevel as ship_service_level, 
    style, 
    sku, 
    category, 
    size, 
    asin, 
    coalesce(courierstatus, 'Cancelled') as courier_status, 
    case 
        when quantity is null then 0 
        else quantity 
    end as quantity,
    coalesce(currency, 'N/A') as currency, 
    case 
        when amount is null then 0 
        else cast(amount as decimal(19,2)) 
    end as amount, 
    coalesce(upper(shipcity), 'N/A') AS ship_city,
    coalesce(upper(shipstate), 'N/A') as ship_state, 
    cast(shippostalcode as int) as ship_postal_code, 
    shipcountry as ship_country, 
    promotionids as promotion_ids, 
    b2b, 
    coalesce(fulfilledby, 'N/A') as fulfilled_by
FROM cleansed_amazon_sales_report
where rnk = 1