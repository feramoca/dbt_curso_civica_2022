{{
  config(
    materialized='view'
  )
}}

WITH src_tiempo AS (
    SELECT * 
    FROM {{ source('ab_schema', 'tiempo3') }}
    ),

tiempo AS (
    SELECT
          estacion
        , h05
        , h06
        , h07
        
    FROM src_tiempo
    where magnitud = 83
    )

SELECT * FROM tiempo
