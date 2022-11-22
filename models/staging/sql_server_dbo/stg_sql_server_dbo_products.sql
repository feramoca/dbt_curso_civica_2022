{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

products AS (
    SELECT
          product_id
        , name
        , inventory
        , price
        , case 
            when inventory > 75 then 'High'
            when inventory > 50 then 'Optimus'
            else 'Low'
            end as Stock



    FROM src_products
    )

SELECT * FROM products