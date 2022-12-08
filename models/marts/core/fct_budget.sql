WITH

src_budget AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets_budget') }}
    ),

ventas_articulos AS (

            with
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

    ),      

budget AS (
    SELECT
          year(month)*100+month(month) as id_anio_mes
        , src_budget.product_id
        , src_budget.quantity as Objetivo_Cantidad
        , ventas_articulos.Unidades_vendidas
        , concat ( 
            cast (((ventas_articulos.Unidades_vendidas*100)/(src_budget.quantity))as numeric (8,2))
            , ' %')  as Consecucion_objetivo

    FROM src_budget
    left join ventas_articulos on (src_budget.product_id = ventas_articulos.product_id) and ( (year(src_budget.month)*100+month(src_budget.month))    = ventas_articulos.id_anio_mes)
    

    )

SELECT * FROM budget

