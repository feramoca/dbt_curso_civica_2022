WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ),

lineas AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ),

Clientes_agregados AS (
    SELECT
        user_id
        , count (pedidos.order_id) as Numero_Pedidos_Cliente
        , sum (order_total) as Importe_Total_Cliente
        , avg (order_total) as Importe_Medio_Pedido_Cliente
        , sum (quantity) as Total_Unidades_Compradas_Cliente
        , count (product_id) as Articulos_Cliente
        , count (distinct product_id) as Articulos_Distintos_Cliente
        , count (distinct address_id) as Direcciones_Distintas_Cliente
    FROM pedidos
    join lineas on lineas.order_id = pedidos.order_id
    group by user_id

    

    )

SELECT * FROM Clientes_agregados