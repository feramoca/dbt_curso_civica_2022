WITH

src_estados AS (
    SELECT * 
    FROM {{ ref('estados') }}
    ), 




states  AS (
    SELECT
          md5 (replace ( state, ' ', '')) as state_id
        , state
        , Postal_Abbreviation
        , Estacion
    FROM src_estados
    )




SELECT * FROM states