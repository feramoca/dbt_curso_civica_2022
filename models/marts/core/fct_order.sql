{{
  config(
    materialized='table'
  )
}}

WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ),

lineas_pedido AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ),

productos as (
    SELECT *
    from {{ref('stg_sql_server_dbo_products')}}
    ),

promos as (
    SELECT *
    from {{ref('stg_sql_server_dbo_promos')}}
    ),

Ped_agreg as (
    SELECT *
    from {{ ref('pedidos_agregados')}}
),    

cli_agreg as (
    SELECT *
    from {{ ref('clientes_agregados')}}
),  

Pedidos_Cliente AS (
    SELECT
          pedidos.order_id
        , created_at as Fecha_Pedido
        , promo_id
--- Importes
        , pedidos.order_total as Pedido_Total_EUR
        , pedidos.order_cost as Pedido_Coste_EUR
        , pedidos.shipping_cost as Pedido_Envio_EUR
        , promos.discount Pedido_Descuento_EUR        
        , pedidos.order_total - pedidos.order_cost as Pedido_Beneficio
--- Datos cliente
        , pedidos.user_id as Cliente
        , address_id as Direccion_Envio

        , cli_agreg.Numero_Pedidos_Cliente
        , cli_agreg.Importe_Total_Cliente
        , cli_agreg.Importe_Medio_Pedido_Cliente
        , cli_agreg.Total_Unidades_Compradas_Cliente
        , cli_agreg.Articulos_Cliente
        , cli_agreg.Articulos_Distintos_Cliente
        , cli_agreg.Direcciones_Distintas_Cliente


--- Articulos
        , lineas_pedido.product_id 
        , lineas_pedido.quantity as Producto_Cantidad
        , productos.price as Producto_Precio
        , productos.inventory as Producto_Inventario
        , ped_agreg.Numero_Articulos_Pedido
        , ped_agreg.Total_Unidades_Pedido
        , ped_agreg.Importe_Medio_Articulo
--- Envíos
        , tracking_id as Pedido_tracking
        , pedidos.status as Pedido_Estado
        , shipping_service as Agencia_Envio
        , estimated_delivery_at as Fecha_Prevista_Envio
        , ped_agreg.dias_en_enviar

    FROM pedidos
    join lineas_pedido on lineas_pedido.order_id = pedidos.order_id
    join productos on productos.product_id = lineas_pedido.product_id
    left join promos on promos.id_promo = pedidos.promo_id
    join ped_agreg on ped_agreg.order_id = pedidos.order_id
    join cli_agreg on cli_agreg.user_id = pedidos.user_id
    )

SELECT * FROM Pedidos_Cliente