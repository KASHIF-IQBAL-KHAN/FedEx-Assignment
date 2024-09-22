WITH raw_amazon_sales_report AS(
SELECT
  *
FROM
  {{ source('fedex', 'amazon_sales_report') }}
)

SELECT
    index, 
    orderid, 
    date, 
    status, 
    fulfilment, 
    saleschannel, 
    shipservicelevel, 
    style, 
    sku, 
    category, 
    size, 
    asin, 
    courierstatus, 
    quantity, 
    currency, 
    amount, 
    shipcity, 
    shipstate, 
    shippostalcode, 
    shipcountry, 
    promotionids, 
    b2b, 
    fulfilledby
FROM raw_amazon_sales_report