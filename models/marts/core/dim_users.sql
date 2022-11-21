{{
  config(
    materialized='table'
  )
}}

WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_users') }}
    ),

dim_usuario AS (
    SELECT
          user_id
        , phone_number
        , first_name
        , last_name
        , cast (created_at as date) as created_at
        , address_id
        , cast (updated_at as date) as updated_at
        , email
    FROM stg_users
    )

SELECT * FROM dim_usuario
