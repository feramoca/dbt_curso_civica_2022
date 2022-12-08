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

dim_fecha_int as (
    select *
    from {{ ref('int_fecha_pedido')}}
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
    FROM {{ ref('stg_seeds_ace') }}
    ), 



Pedidos_Cliente AS (
    SELECT
          pedidos.order_id
        , pedidos.created_at
        , dim_fecha_int.Fecha_Pedido_id as Fecha_Pedido_id
        , promo_id
        , pedidos.user_id as Cliente
--- Importes
        , pedidos.order_total as Pedido_Total_USD
        , pedidos.order_cost as Pedido_Coste_USD
        , pedidos.shipping_cost as Pedido_Envio_USD
        , promos.discount Pedido_Descuento_USD        
--- Articulos 
        , ped_agreg.Numero_Articulos_Pedido
        , ped_agreg.Total_Unidades_Pedido
        , cast (ped_agreg.Importe_Medio_Articulo as numeric (8,2)) as Importe_Medio_Articulo
--- Envíos
        , pedidos.address_id as Direccion_Envio
        , tracking_id as Pedido_tracking
        , pedidos.status_id as Pedido_Estado
        , shipping_service_id as Agencia_Envio
        , dim_fecha_int.Fecha_entrega_id
        , dim_fecha_int.Fecha_Prevista_Entrega_id
        , ped_agreg.Dias_en_Entregar
--- Temp
        , stg_weather.temperatura_valor
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
    join dim_addresses on dim_addresses.address_id = pedidos.address_id
    join dim_state on dim_state.state_id = dim_addresses.state_id    
    join dim_fecha_int on dim_fecha_int.order_id = pedidos.order_id
    left join stg_weather on (stg_weather.estacion = dim_state.estacion) and (stg_weather.hora = hour (pedidos.created_at)) and (stg_weather.fecha_inventada = dim_fecha_int.Fecha_Pedido_id)   
    join src_ace on src_ace.state_id = dim_state.state_id

--where fechacreacion.id_date = 20210211
--where pedidos.order_id = '5b13b820-a450-42d2-aaaa-a8a9c5fbd48c'
--order by pedidos.order_id desc
    )

SELECT * FROM Pedidos_Cliente