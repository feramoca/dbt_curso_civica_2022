---- no hace nada estoy trabajando

{% macro temperatura_valor(table, column) %}
    {% set query_sql %}
    SELECT DISTINCT {{column}} FROM {{table}}
    {% endset %}



   {% if execute %}
        {% set column_values =  run_query(query_sql).columns[0].values()%}



       {% for column_value in column_values %}
            ( {{column}} = '{{column_value}}' ) AS {{column_value}}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    {% else %}



   {% endif %}




{% endmacro %}