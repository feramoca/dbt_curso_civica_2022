
WITH

eventos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_events') }}
    ),

Eventos_web AS (
    SELECT
          session_id
        , created_at
        , user_id  
        , product_id
        ,{{column_values_to_metrics(ref('stg_sql_server_dbo_events'),'event_type')}}         
    FROM eventos

    group by 1,2,3,4

    )

SELECT * FROM Eventos_web