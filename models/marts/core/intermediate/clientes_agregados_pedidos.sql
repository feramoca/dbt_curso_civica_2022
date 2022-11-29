WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ),

Clientes_agregados AS (
    SELECT
        user_id
        , count (pedidos.order_id) as Numero_Pedidos_Cliente
        , sum (order_total) as Importe_Total_Cliente
        , avg (order_total) as Importe_Medio_Pedido_Cliente
        , max (order_total) as Importe_Pedido_Max
        , min (order_total) as Importe_Pedido_Min
        , count (distinct address_id) as Direcciones_Distintas_Cliente
        , max (created_at) as Fecha_ultimo_pedido
        
    FROM pedidos
    -- quitar el where, es para hacer pruebas
    --where user_id = '93908a68-20b7-44ba-b6c1-6f4cc57d7726'
    group by user_id
    )

SELECT * FROM Clientes_agregados