{{
  config(
    materialized='table'
  )
}}

WITH stg_products AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_products') }}
    ),

dim_products AS (
    SELECT
          product_id
        , name
        , Stock
    FROM stg_products
    )

SELECT * FROM dim_products