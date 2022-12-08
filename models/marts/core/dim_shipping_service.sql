WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ), 

agencias AS (
    SELECT
          distinct shipping_service_id
        , shipping_service
    FROM pedidos
    
    )

SELECT * FROM agencias