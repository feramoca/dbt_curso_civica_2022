WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ), 


dim_fecha as (
    select *
    from {{ ref('dim_fecha')}}
),

dim_addresses as (
    select *
    from {{ ref('dim_addresses')}}
),

dim_state as (
    select *
    from {{ ref('dim_state')}}
),

dim_kk as (
    select *
    from {{ ref('dim_kk_v2')}}
),

Pedidos_Cliente AS (
    SELECT
          order_id
        --, created_at as Fecha_Pedido
        , fechacreacion.id_date as Fecha_Pedido_id
        , dim_state.estacion
--- Envíos
        , pedidos.address_id 
        --, dim_tiempo.Estacion
--- borrar abajo
        , hour (pedidos.created_at) as hora_pedido        
        , cast((hour (pedidos.created_at)) as int) as hora_pedido2                
        --, dim_kk.kk
        , value
    FROM pedidos

    join dim_fecha fechacreacion on fechacreacion.fecha = cast (pedidos.created_at as date)
    join dim_addresses on dim_addresses.address_id = pedidos.address_id
    join dim_state on dim_state.state_id = dim_addresses.state_id
    --join dim_kk on (dim_kk.estacion = dim_state.estacion) and (dim_kk.fecha_inventada = fechacreacion.id_date)
    left join dim_kk on (dim_kk.estacion = dim_state.estacion) and (dim_kk.hora = hour (pedidos.created_at)) and (dim_kk.fecha_inventada = fechacreacion.id_date)
       
where fechacreacion.id_date = 20210211
    )

SELECT * FROM Pedidos_Cliente