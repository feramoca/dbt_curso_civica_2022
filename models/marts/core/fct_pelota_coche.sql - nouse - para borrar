--tengo que revisar las fechas, que me quitan registros, son las marcadas con 4- ----

WITH

tabla_a AS (
    SELECT * 
    FROM {{ ref('stg_seeds_a') }}
    ), 

tabla_b as (
    SELECT *
    from {{ ref('stg_seeds_b')}}
    ),




resultado AS (
    SELECT
          tabla_a.nombre
        , tabla_a.color
        , valor


    FROM tabla_a
    left join tabla_b on (tabla_b.nombre = tabla_a.nombre) and (tabla_b.color = tabla_a.color)


    )

SELECT * FROM resultado