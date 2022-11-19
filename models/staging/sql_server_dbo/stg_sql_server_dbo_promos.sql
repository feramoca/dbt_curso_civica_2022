{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

promos AS (
    SELECT
          md5 (replace ( promo_id, ' ', '')) as id_promo
        , promo_id as promo_nombre
        , discount
        , status
        
    FROM src_promos
    )

SELECT * FROM promos