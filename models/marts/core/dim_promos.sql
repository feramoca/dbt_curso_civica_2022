{{
  config(
    materialized='table'
  )
}}

WITH stg_promos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_promos') }}
    ),

dim_promos AS (
    SELECT
          id_promo
        , promo_nombre
        , discount
        , status
    FROM stg_promos
    )

SELECT * FROM dim_promos