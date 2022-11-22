{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

users AS (
    SELECT
          user_id
        , phone_number
        , first_name
        , last_name
        , cast (created_at as date) as created_at
        , address_id
        , cast (updated_at as date) as updated_at
        , email
        
        
    FROM src_users
    )

SELECT * FROM users