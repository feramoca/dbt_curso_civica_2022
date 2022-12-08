WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

events AS (
    SELECT
          event_id
        , cast (created_at as datetime) as created_at
        , case 
            when product_id = '' then null
            else product_id
            end as product_id
        , session_id
        , page_url
        , order_id
        , event_type
        , user_id
        
    FROM src_events
    )

SELECT * FROM events 