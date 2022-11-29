{{
  config(
    materialized='table'
  )
}}

WITH

eventos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_events') }}
    ),

usuarios AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_users') }}
    ),       

dim_fecha as (
    select *
    from {{ ref('dim_fecha')}}
),    

Eventos_web AS (
    SELECT
          session_id
        , eventos.user_id  
        , usuarios.first_name
        , email        
        , count (distinct page_url) as Paginas_Vistas
        , count (distinct eventos.product_id) as Productos_vistos
        , min ( eventos.created_at) as Fecha_Primera_Visita_Web
        , max ( eventos.created_at) as Fecha_Ultima_Visita_Web
        , DATEDIFF(MINUTE, Fecha_Primera_Visita_Web, Fecha_Ultima_Visita_Web) AS Sesion_minutos
        ,{{column_values_to_metrics(ref('stg_sql_server_dbo_events'),'event_type')}}       

    FROM eventos
    join usuarios on usuarios.user_id = eventos.user_id
    group by session_id, eventos.user_id, usuarios.first_name, email 
    )

SELECT * FROM Eventos_web