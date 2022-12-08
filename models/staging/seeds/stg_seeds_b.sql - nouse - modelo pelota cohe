WITH

src_b AS (
    SELECT * 
    FROM {{ ref('b') }}
    ), 


bb  AS (
    SELECT
    id_b,nombre,color,valor
    FROM src_b
    )

SELECT * FROM bb