WITH

src_order_items AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo_order_items') }}
    ),

int_fecha_pedido AS (
    SELECT * 
    FROM {{ ref('int_fecha_pedido') }}
    ),      

o_i_month AS (
    SELECT
          product_id
        , sum (quantity) as Unidades_vendidas
        , id_anio_mes
    FROM src_order_items
    join int_fecha_pedido on int_fecha_pedido.order_id = src_order_items.order_id
    group by product_id, id_anio_mes
        )

SELECT * FROM o_i_month

