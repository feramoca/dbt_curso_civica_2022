WITH

src_a AS (
    SELECT * 
    FROM {{ ref('a') }}
    ), 


aa  AS (
    SELECT
      id_a,nombre,color
    FROM src_a
    )

SELECT * FROM aa