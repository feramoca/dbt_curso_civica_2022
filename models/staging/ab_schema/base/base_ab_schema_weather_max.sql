
------- NO BORRAR --------------
------este funciona, se queda con lo últimos valores de cada día

WITH 

src_a AS (
    SELECT * 
        from {{ ref('base_ab_schema_weather')}} 
    ),

src_b AS (
    SELECT 
          DISTINCT (punto_muestreo) as punto_muestreo
        , ano
        , mes
        , dia
        , MAX(_AIRBYTE_EMITTED_AT) AS _AIRBYTE_EMITTED_AT
        from {{ ref('base_ab_schema_weather')}} 
    GROUP BY 1,2,3,4
    ),



tiempo AS (
    SELECT
          src_a.ano
        , src_a.mes
        , src_a.dia
        --, provincia
        , estacion
        , src_a.punto_muestreo
        --, magnitud
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
        --, src_a._AIRBYTE_EMITTED_AT
        , {{ dbt_utils.surrogate_key('src_a.ano', 'src_a.mes', 'src_a.dia', 'src_a.punto_muestreo') }} as id_temp
        
        
    FROM src_a 
        left join src_b on src_b.punto_muestreo = src_a.punto_muestreo
        
    where 
        src_a._AIRBYTE_EMITTED_AT = src_b._AIRBYTE_EMITTED_AT
        and magnitud = 83
    )

SELECT * FROM tiempo





         