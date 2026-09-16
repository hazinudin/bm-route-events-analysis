{% macro rni_source(semester, year) %}
    {%- if not execute -%}
        {{ return("SMD.RNI_" ~ semester ~ "_" ~ year) }}
    {%- endif -%}

    {%- set identifier = 'RNI_' ~ semester ~ '_' ~ year -%}
    {%- set rni_relation = adapter.get_relation(target.database, 'SMD', identifier) -%}

    {%- if rni_relation is none -%}
        {{ exceptions.raise_compiler_error(
            "Source table SMD." ~ identifier ~ " does not exist. Aborting build."
        ) }}
    {%- endif -%}

    {{ return(rni_relation) }}
{% endmacro %}
