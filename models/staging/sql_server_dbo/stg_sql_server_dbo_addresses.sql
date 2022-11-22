{{
  config(
    materialized='view'
  )
}}

WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

addresses AS (
    SELECT
          address_id
        , country
        , zipcode
        , address
        , state
        
    FROM src_addresses
    )

SELECT * FROM addresses