
WITH

tmp AS (
{{ dbt_utils.unpivot(ref('base_ab_schema_tiempo'), cast_to='float', exclude=['ano','mes','dia','provincia','estacion','punto_muestreo','magnitud']) }}
    ), 

tem AS (
    SELECT
        --  *
          concat (ano,mes,'0',dia) as id_fecha
        , cast ((replace ( field_name, 'H', '')) as integer) as hora
        , '20210211' as fecha_inventada
        --, ano
        --, mes
        --, dia
        , estacion
        , value as temperatura_valor
    FROM tmp
    where value <> 0
    )

SELECT * FROM tem




