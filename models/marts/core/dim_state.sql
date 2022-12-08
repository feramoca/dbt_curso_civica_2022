
WITH

dim_state AS (
    SELECT * 
    FROM {{ ref('stg_seeds_states') }}
    ), 

estate AS (
    SELECT
          state_id
        , state
        , Postal_Abbreviation
        , Estacion
    FROM dim_state
    
    )

SELECT * FROM estate