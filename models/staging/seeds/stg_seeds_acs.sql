{{
  config(
    materialized='view'
  )
}}

WITH

src_ace AS (
    SELECT * 
    FROM {{ ref('ace') }}
    ), 

CE_shipping_service  AS (
    SELECT
          md5 (replace ( state, ' ', '')) as state_id 
        , state
        , fijo
        , variable
        , zona
    FROM src_ace
    )

SELECT * FROM CE_shipping_service