WITH

tmp AS (
{{ dbt_utils.unpivot(ref('base_ab_schema_weather_max'), cast_to='float', exclude=['ano','mes','dia','estacion','punto_muestreo','id_temp']) }}
    ), 

tem AS (
    SELECT
          id_temp
        , concat (ano,mes,'0',dia) as id_fecha
        , cast ((replace ( field_name, 'H', '')) as integer) as hora
        , estacion
        , value as temperatura_valor
        , case 
            when id_fecha = 20221205 then '20210210'
            when id_fecha = 20221206 then '20210211'
            else id_fecha
            end as fecha_inventada        
--- reemplazar la de abajo por la de arriba cuando sea domingo 11 diciembre 2022
        , (id_fecha - 11000) as fecha_inventada_2
    FROM tmp
    where value <> 0
    )

SELECT * FROM tem
--where id_temp != 'd05b2f9f14d369427f360b1b900ff96d'



