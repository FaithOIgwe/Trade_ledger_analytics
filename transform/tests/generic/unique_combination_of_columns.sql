{#
  Small version of dbt_utils.unique_combination_of_columns.
  Kept local so the project has no package dependencies.
#}
{% test unique_combination_of_columns(model, columns) %}

select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
