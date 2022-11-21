{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

orders AS (
    SELECT
          order_id
        , md5 (replace ( promo_id, ' ', '')) as promo_id
        , promo_id as promo
        , order_cost
        , cast (created_at as date) as created_at
        , tracking_id
        , address_id
        , cast (delivered_at as date) as delivered_at
        , status
        , shipping_cost
        , user_id
        , shipping_service
        , order_total
        , cast (estimated_delivery_at as date) as estimated_delivery_at
        
    FROM src_orders
    )

SELECT * FROM orders

