{% macro safe_cast(column, data_type) %}
    {# Athena uses try_cast to return NULL instead of failing on bad data #}
    try_cast({{ column }} as {{ data_type }})
{% endmacro %}