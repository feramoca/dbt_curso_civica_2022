WITH

stg_lineas_pedido AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ),

stg_productos as (
    SELECT *
    from {{ref('stg_sql_server_dbo_products')}}
    ),


fct_produts AS (
    SELECT
          order_id
        , stg_lineas_pedido.product_id
        , quantity
        , price
        , (quantity * price) as total_linea

    FROM stg_lineas_pedido
    join stg_productos on stg_productos.product_id = stg_lineas_pedido.product_id
    )

SELECT * FROM fct_produts