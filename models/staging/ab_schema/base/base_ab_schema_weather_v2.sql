
WITH src_tiempo AS (
    SELECT * 
    FROM {{ source('ab_schema', 'weather') }}
    ),

tiempo AS (
    SELECT
          ano
        , mes
        , dia
        , provincia
        , estacion
        , punto_muestreo
        , magnitud
        , h01
        , h02
        , h03
        , h04
        , h05
        , h06
        , h07
        , h08
        , h09
        , h10 
        , h11
        , h12
        , h13
        , h14
        , h15
        , h16
        , h17
        , h18
        , h19
        , h20 
        , h21 
        , h22
        , h23
        , h24
        , _AIRBYTE_EMITTED_AT
        , {{ dbt_utils.surrogate_key(['ano','mes','dia','provincia','estacion','punto_muestreo','magnitud']) }} as id_temp
        
    FROM src_tiempo
    where magnitud = 83
    )

SELECT * FROM tiempo





         