WITH

src_a AS (
    SELECT * 
    FROM {{ ref('prueba_incremental') }}
    ), 

src_b AS (
    SELECT 
        DISTINCT id
        , dia
        , MAX(hora) AS hora
    FROM {{ ref('prueba_incremental') }}
    GROUP BY 1,2
    ),

aa  AS (
    SELECT
        a.id
      , a.dia
      , a.hora
      , a.v1
      , a.v2
    FROM src_a a
        LEFT JOIN src_b b
        ON a.id = b.id 

    WHERE a.hora = b.hora

    )

SELECT * FROM aa