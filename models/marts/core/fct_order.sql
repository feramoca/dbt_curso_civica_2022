--tengo que revisar las fechas, que me quitan registros, son las marcadas con 4- ----

WITH

pedidos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_orders') }}
    ), 

promos as (
    SELECT *
    from {{ref('stg_sql_server_dbo_promos')}}
    ),

Ped_agreg as (
    SELECT *
    from {{ ref('int_pedido_agregado')}} 
),    

cli_agreg as (
    SELECT *
    from {{ ref('int_clientes_agregados_pedidos')}}
),  

cli_agreg as (
    SELECT *
    from {{ ref('int_clientes_agregados_pedidos')}}
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

stg_weather as (
    select *
    from {{ ref('stg_ab_schema_weather')}}
),

src_ace AS (
    SELECT * 
    FROM {{ ref('stg_seeds_acs') }}
    ), 



Pedidos_Cliente AS (
    SELECT
          pedidos.order_id
        --, created_at as Fecha_Pedido
        , created_at
        , fechacreacion.id_date as Fecha_Pedido_id
        , promo_id
        , pedidos.user_id as Cliente
--- Importes
        , pedidos.order_total as Pedido_Total_EUR
        , pedidos.order_cost as Pedido_Coste_EUR
        , pedidos.shipping_cost as Pedido_Envio_EUR
        , promos.discount Pedido_Descuento_EUR        
        , pedidos.order_total - pedidos.order_cost as Pedido_Beneficio

--- Articulos 

        , ped_agreg.Numero_Articulos_Pedido
        , ped_agreg.Total_Unidades_Pedido
        , ped_agreg.Importe_Medio_Articulo 
--- Envíos
        , pedidos.address_id as Direccion_Envio
        , tracking_id as Pedido_tracking
        , pedidos.status_id as Pedido_Estado
        , shipping_service_id as Agencia_Envio
        --, delivered_at as Fecha_entrega
        , fechaentrega.id_date as Fecha_entrega_id
        --, estimated_delivery_at as Fecha_Prevista_Entrega
        , fechaprevista.id_date as Fecha_Prevista_Entrega_id
        , ped_agreg.Dias_en_Entregar
--- Temp
        --, value        
        , stg_weather.temperatura_valor
        
       -- , case 
       --     when stg_weather.temperatura_valor > 10 then 1
       --     when stg_weather.temperatura_valor < 5 then 2
       --     else 0
       --     end as carretera
        --, (Fecha_entrega_id - Fecha_Prevista_Entrega_id) as diferencia_fechas
        --, (Fecha_Pedido_id + zona + carretera) as nueva_fecha_entrega

        , (Fecha_Pedido_id + zona + 
            case 
            when stg_weather.temperatura_valor > 10 then 1
            when stg_weather.temperatura_valor < 5 then 2
            else 0
            end
            ) as nueva_fecha_entrega2

    FROM pedidos
    left join promos on promos.id_promo = pedidos.promo_id
    join ped_agreg on ped_agreg.order_id = pedidos.order_id
    join cli_agreg on cli_agreg.user_id = pedidos.user_id
    join dim_fecha fechacreacion on fechacreacion.fecha = cast (pedidos.created_at as date)
    left join dim_fecha fechaentrega on fechaentrega.fecha = cast (pedidos.delivered_at as date)
    left join dim_fecha fechaprevista on fechaprevista.fecha = cast (pedidos.estimated_delivery_at as date)
    join dim_addresses on dim_addresses.address_id = pedidos.address_id
    join dim_state on dim_state.state_id = dim_addresses.state_id    
    left join stg_weather on (stg_weather.estacion = dim_state.estacion) and (stg_weather.hora = hour (pedidos.created_at)) and (stg_weather.fecha_inventada = fechacreacion.id_date)    
    join src_ace on src_ace.state_id = dim_state.state_id
       

--where fechacreacion.id_date = 20210211
--where pedidos.order_id = '5b13b820-a450-42d2-aaaa-a8a9c5fbd48c'
--order by pedidos.order_id desc
    )

SELECT * FROM Pedidos_Cliente