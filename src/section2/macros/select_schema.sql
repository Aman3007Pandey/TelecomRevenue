{% macro select_schema(source_schema, test_schema) %}

    {% if var('unit_testing',false)==true %}
        {{return(test_schema)}}
    {% else %}
        {{return (source_schema)}}
    {% endif %}
{% endmacro %}            