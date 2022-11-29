
WITH

dim_state AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_addresses') }}
    ), 

estate AS (
    SELECT
          state
          ,'28079056_87_98' as estacion
    FROM dim_state
    
    )

SELECT * FROM estate