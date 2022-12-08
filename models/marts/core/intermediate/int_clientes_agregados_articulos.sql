WITH

lineas AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ),

productos AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo_products') }}
    ),

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ),

Clientes_agregados_articulos AS (
    SELECT
          pedidos.user_id
        , sum (quantity) as Total_Unidades_Compradas_Cliente
        , count (lineas.product_id) as Articulos_Cliente
        , count (distinct lineas.product_id) as Articulos_Distintos_Cliente
        
    FROM lineas
    join productos on productos.product_id = lineas.product_id
    join pedidos on pedidos.order_id = lineas.order_id
    -- quitar el where, es para hacer pruebas
    --where pedidos.user_id = '93908a68-20b7-44ba-b6c1-6f4cc57d7726'
    group by pedidos.user_id
    )

SELECT * FROM Clientes_agregados_articulos