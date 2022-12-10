WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ),

lineas AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ),

direcciones AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_addresses') }}
    ),    
 
Pedidos_agregados AS (
    SELECT
        pedidos.order_id
        , count (pedidos.order_id) as Numero_Articulos_Pedido
        , sum (quantity) as Total_Unidades_Pedido
        , pedidos.order_total
        , Cast (((order_total) / (sum (quantity))) as numeric (8,2)) as Importe_Medio_Articulo
        , DATEDIFF(day, created_at, delivered_at) AS Dias_en_Entregar
        , md5 (replace ( state, ' ', '')) as state_id    
        , shipping_cost
    FROM pedidos
    join lineas on lineas.order_id = pedidos.order_id
    join direcciones on direcciones.address_id = pedidos.address_id
    group by pedidos.order_id, pedidos.order_total, Dias_en_Entregar, state_id, shipping_cost
    order by pedidos.order_id asc
    
    )

SELECT * FROM Pedidos_agregados