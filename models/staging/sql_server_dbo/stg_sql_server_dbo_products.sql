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
        
    FROM src_products
    )

SELECT * FROM products