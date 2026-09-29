{% macro rni_sta_scale(semester, year) %}
    {%- if not execute -%}
        {{ return(1) }}
    {%- endif -%}

    {%- set table_name = 'ROUGHNESS_' ~ semester ~ '_' ~ year -%}
    {%- set rel = adapter.get_relation(target.database, 'SMD', table_name) -%}

    {%- if rel is none -%}
        {{ exceptions.raise_compiler_error(
            "Source table SMD." ~ table_name ~ " does not exist. Aborting build."
        ) }}
    {%- endif -%}

    {%- set min_delta_query -%}
        SELECT MIN(TO_STA - FROM_STA) FROM {{ rel }} WHERE TO_STA > FROM_STA
    {%- endset -%}
    {%- set min_delta = run_query(min_delta_query).columns[0][0] -%}

    {%- if min_delta is none -%}
        {{ exceptions.raise_compiler_error(
            "Cannot determine FROM_STA units for SMD." ~ table_name
            ~ " (no rows with TO_STA > FROM_STA). Aborting build."
        ) }}
    {%- elif min_delta < 1 -%}
        {{ return(100) }}
    {%- else -%}
        {{ return(1) }}
    {%- endif -%}
{% endmacro %}
