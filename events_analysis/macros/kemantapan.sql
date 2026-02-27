{% macro km_sum(alias, surf_types, iri_min, iri_max, column_name, sk_len_col, grading_column) %}
SUM(
  CASE
    WHEN {{ alias }}.SURF_TYPE IN ({{ surf_types | join(',') }})
     {% if iri_min is not none %} AND {{ alias }}.{{grading_column}} > {{ iri_min }} {% endif %}
     {% if iri_max is not none %} AND {{ alias }}.{{grading_column}} <= {{ iri_max }} {% endif %}
    THEN {{ alias }}.SEGMENT_LENGTH
    ELSE 0
  END
){% if sk_len_col is not none %}*MAX({{sk_len_col}})/SUM({{alias}}.SEGMENT_LENGTH) {% endif %}
AS {{ column_name }}
{% endmacro %}

{% macro km_sum_inclusive(alias, surf_types, iri_min, iri_max, column_name, sk_len_col, grading_column) %}
SUM(
  CASE
    WHEN {{ alias }}.SURF_TYPE IN ({{ surf_types | join(',') }})
     {% if iri_min is not none %} AND {{ alias }}.{{grading_column}} >= {{ iri_min }} {% endif %}
     {% if iri_max is not none %} AND {{ alias }}.{{grading_column}} <= {{ iri_max }} {% endif %}
    THEN {{ alias }}.SEGMENT_LENGTH
    ELSE 0
  END
){% if sk_len_col is not none %}*MAX({{sk_len_col}})/SUM({{alias}}.SEGMENT_LENGTH) {% endif %}
AS {{ column_name }}
{% endmacro %}
 