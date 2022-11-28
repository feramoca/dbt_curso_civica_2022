
WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ), 

status AS (
    SELECT
          distinct status_id
        , status
    FROM pedidos
    
    )

SELECT * FROM status