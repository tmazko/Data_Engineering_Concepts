{% macro calculate_profit(price, cost) %}
    ({{ price }} - {{ cost }})
{% endmacro %}