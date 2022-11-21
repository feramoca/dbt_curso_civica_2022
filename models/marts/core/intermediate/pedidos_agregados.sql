WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ),

lineas AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ),

Pedidos_agregados AS (
    SELECT
        pedidos.order_id
        , count (pedidos.order_id) as Numero_Articulos_Pedido
        , sum (quantity) as Total_Unidades_Pedido
        , pedidos.order_total
        , ((order_total) / (sum (quantity))) as Importe_Medio_Articulo
        , DATEDIFF(day, created_at, estimated_delivery_at) AS Dias_en_Enviar
    FROM pedidos
    join lineas on lineas.order_id = pedidos.order_id
    group by pedidos.order_id, pedidos.order_total, Dias_en_Enviar
    order by pedidos.order_id asc
    
    )

SELECT * FROM Pedidos_agregados