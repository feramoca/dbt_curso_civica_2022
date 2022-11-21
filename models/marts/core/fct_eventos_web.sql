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

Eventos_web AS (
    SELECT
          session_id
        , eventos.product_id
        , eventos.user_id  
        , count (page_url) as Paginas_Vistas
        , count (eventos.product_id) as Productos_vistos
        , eventos.order_id
        , eventos.created_at as Fecha_Visita_Web

    FROM eventos
    group by session_id, eventos.product_id, eventos.user_id, eventos.order_id, eventos.created_at

    )

SELECT * FROM Eventos_web