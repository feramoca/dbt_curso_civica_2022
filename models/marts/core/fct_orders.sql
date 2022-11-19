WITH pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ),


Pedidos_Cliente AS (
    SELECT
          user_id
        , count (user_id) as numero_pedidos
        , sum (order_total) as Importe_total

    FROM pedidos
    group by user_id
    )

SELECT * FROM Pedidos_Cliente
