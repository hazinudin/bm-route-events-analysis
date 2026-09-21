{% macro rni_sources() %}
    {%- if not execute -%}
        {{ return([]) }}
    {%- endif -%}

    {%- set query -%}
        SELECT table_name
        FROM all_tables
        WHERE owner = 'SMD'
          AND table_name LIKE 'RNI_%'
    {%- endset -%}

    {%- set result = run_query(query) -%}
    {%- set rnis = [] -%}

    {%- for row in result -%}
        {%- set identifier = row[0] -%}
        {%- set match = modules.re.match('RNI_([12])_([0-9]{4})$', identifier) -%}
        {%- if match -%}
            {%- do rnis.append({
                'semester': match.group(1) | int,
                'year': match.group(2) | int,
                'relation': api.Relation.create(
                    database=target.database,
                    schema='SMD',
                    identifier=identifier,
                ),
            }) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if rnis | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "No RNI_<semester>_<year> tables found in schema SMD. Aborting full refresh."
        ) }}
    {%- endif -%}

    {# Stable double sort: year first, then semester within year #}
    {{ return(rnis | sort(attribute='semester') | sort(attribute='year')) }}
{% endmacro %}
