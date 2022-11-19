{{
  config(
    materialized='view'
  )
}}

WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

events AS (
    SELECT
          event_id
        , cast (created_at as date) as created_at
        , product_id
        , session_id
        , page_url
        , order_id
        , event_type
        , user_id
        
    FROM src_events
    )

SELECT * FROM events 