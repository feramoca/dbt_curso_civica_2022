WITH

stg_lineas_pedido AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ),

stg_productos as (
    SELECT *
    from {{ref('stg_sql_server_dbo_products')}}
    ),

int_fecha_pedido AS (
    SELECT * 
    FROM {{ ref('int_fecha_pedido') }}
    ),    


fct_produts AS (
    SELECT
          {{ dbt_utils.surrogate_key(['stg_lineas_pedido.order_id', 'stg_lineas_pedido.product_id']) }} as order_item_id
---fechas
        , created_at
        , Fecha_Pedido_id
        , id_anio_mes

        --, Fecha_Pedido_id  
        , stg_lineas_pedido.order_id
        , stg_lineas_pedido.product_id
        , quantity
        , price
        , (quantity * price) as total_linea

    FROM stg_lineas_pedido
    join stg_productos on stg_productos.product_id = stg_lineas_pedido.product_id
    join int_fecha_pedido on int_fecha_pedido.order_id = stg_lineas_pedido.order_id
    )

SELECT * FROM fct_produts