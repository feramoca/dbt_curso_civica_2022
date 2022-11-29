---- no hace nada estoy trabajando

{% macro temperatura_valor(Estado,Fecha,hora) %}

    {% set query_sql %}

    SELECT {{hora}} FROM {{ ref('stg_ab_schema_tiempo') }} a
    join {{ ref('stg_ab_schema_tiempo') }} b on b.estado = a.estado and b.fecha = a.fecha
    where
    a.estado = {{estado}}
    and
    a.fecha = {{fecha}}
   
    {% endset %}

{% endmacro %}