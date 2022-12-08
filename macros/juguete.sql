---- no hace nada estoy trabajando


{% macro juguete(table, column) %}
    {% set query_sql %}
    SELECT DISTINCT {{column}} FROM {{table}}
    {% endset %}



   {% if execute %}
        {% set column_values =  run_query(query_sql).columns[0].values()%}



       {% for column_value in column_values %}
            SUM(CASE WHEN {{column}} = '{{column_value}}' THEN 1 ELSE 0 END) AS {{column_value}}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    {% else %}



   {% endif %}




{% endmacro %}