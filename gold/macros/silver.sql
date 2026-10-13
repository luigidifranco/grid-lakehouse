{#- Lee una tabla silver: Delta en el Mac; Parquet solo para pruebas sin la extensión delta. -#}
{% macro silver(tabla) -%}
  {%- if var('silver_format') == 'delta' -%}
    delta_scan('{{ var("data_dir") }}/silver/{{ tabla }}')
  {%- else -%}
    read_parquet('{{ var("data_dir") }}/silver_parquet/{{ tabla }}/*.parquet')
  {%- endif -%}
{%- endmacro %}

{#- Nombre de esquema tal cual (staging, gold, reference), sin prefijo del target. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}

{#- Hora local de Bruselas (sin zona) a partir de un instante UTC. -#}
{% macro hora_bruselas(col) -%}
  timezone('Europe/Brussels', {{ col }})
{%- endmacro %}
