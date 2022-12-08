
WITH

or_it AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ), 

enlace AS (
    SELECT
          {{ dbt_utils.surrogate_key(['order_id', 'product_id']) }} as order_item_id
        , order_id
        , product_id
    FROM or_it
    
    )

SELECT * FROM enlace