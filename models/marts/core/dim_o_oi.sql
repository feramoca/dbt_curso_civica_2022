
WITH

or_it AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ), 

enlace AS (
    SELECT
          order_id
        , product_id
    FROM or_it
    
    )

SELECT * FROM enlace