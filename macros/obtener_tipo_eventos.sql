{% macro obtener_valores(table, column) %}
  --Preparamos la query
  {% set query_sql %}
    SELECT DISTINCT {{column}} FROM {{table}}
    {% endset %}
    --Ejecutamos la query    
    {% set results = run_query(query_sql) %}
    --En caso de que se ejecute
    {% if execute %}
    --Devolvemos los valores de la primera columna
        {% set results_list = results.columns[0].values() %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

{{ return(results_list) }}
{% endmacro %}