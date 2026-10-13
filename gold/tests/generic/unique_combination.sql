{#- Falla si alguna combinación de columnas se repite (clave compuesta). -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as repeticiones
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
