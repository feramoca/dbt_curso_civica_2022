WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ), 

dim_fecha as (
    select *
    from {{ ref('dim_fecha')}}
),

Pedidos_Cliente AS (
    SELECT
          pedidos.order_id
        --, created_at as Fecha_Pedido
        , created_at
        , fechacreacion.id_date as Fecha_Pedido_id

--- Envíos
        , fechaentrega.id_date as Fecha_entrega_id
        --, estimated_delivery_at as Fecha_Prevista_Entrega
        , fechaprevista.id_date as Fecha_Prevista_Entrega_id
        , fechacreacion.id_anio_mes as id_anio_mes

    FROM pedidos
    join dim_fecha fechacreacion on fechacreacion.fecha = cast (pedidos.created_at as date)
    left join dim_fecha fechaentrega on fechaentrega.fecha = cast (pedidos.delivered_at as date)
    left join dim_fecha fechaprevista on fechaprevista.fecha = cast (pedidos.estimated_delivery_at as date)

--where fechacreacion.id_date = 20210211
--where pedidos.order_id = '5b13b820-a450-42d2-aaaa-a8a9c5fbd48c'
--order by pedidos.order_id desc
    )

SELECT * FROM Pedidos_Cliente