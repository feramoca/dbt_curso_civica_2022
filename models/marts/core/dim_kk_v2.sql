-- Este funciona junto con fct_order_t.sql

WITH


dim_tiempo as (
    select *
    from {{ ref('stg_ab_schema_tiempo')}}
),

aa AS (
    SELECT
          estacion
        --, created_at as Fecha_Pedido
        , '20210211' as fecha_inventada
        , hora
        --, avg (value) as kk
        , value
    FROM dim_tiempo
    --group by id_fecha, estacion

    )



SELECT * FROM aa