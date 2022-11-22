{{
  config(
    materialized='table'
  )
}}

WITH stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_addresses') }}
    ),

dim_addresses AS (
    SELECT
          address_id
        , country
        , zipcode
        , address
        , state
    FROM stg_addresses
    )

SELECT * FROM dim_addresses