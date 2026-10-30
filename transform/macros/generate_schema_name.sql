{#
  dbt's default glues the custom schema onto the target schema
  (analytics_staging). I'd rather the schemas be exactly what the
  folder config says: staging, intermediate, analytics, snapshots.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
