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
        , case 
            when promo_id = '' then null
            else md5 (replace ( promo_id, ' ', ''))
            end as promo_id
        , order_cost
        , cast (created_at as datetime) as created_at
        , tracking_id
        , address_id
        , cast (delivered_at as datetime) as delivered_at
        , status
        , md5 (status) as status_id
        , shipping_cost
        , user_id
        , shipping_service
        , md5 (shipping_service) as shipping_service_id        
        , order_total
        , cast (estimated_delivery_at as datetime) as estimated_delivery_at
        
    FROM src_orders
    )

SELECT * FROM orders

