
WITH

tmp AS (
{{ dbt_utils.unpivot(ref('base_ab_schema_tiempo'), cast_to='varchar', exclude=['ano','mes','dia','provincia','estacion','punto_muestreo','magnitud']) }}
    ), 

tem AS (
    SELECT
          *
        , concat (ano,mes,dia) as id_fecha
        --, (replace ( field_name, 'H', '')) as hora
        ,  cast ((replace ( field_name, 'H', '')) as integer) as hora2
    FROM tmp
    where value <> 0
    )

SELECT * FROM tem