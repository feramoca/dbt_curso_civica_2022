{{
  config(
    materialized='view'
  )
}}

WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

order_items AS (
    SELECT
          order_id
        , product_id
        , quantity
        
    FROM src_order_items
    )

SELECT * FROM order_items