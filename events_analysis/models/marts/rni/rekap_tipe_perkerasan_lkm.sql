{{
    config(
        materialized='incremental',
        unique_key=['LINKID', 'YEAR'],
        incremental_strategy='delete+insert'
    )
}}

{%- set routes = var('routes', none) -%}

SELECT 
sde.gdb_util.next_rowid({{"'"+this.schema+"'"}}, {{"'"+this.name+"'"}}) as OBJECTID,
t1.LINKID,
MAX({{ prov_name_column() }}) as BM_PROV_ID, 
SUM(t1.SEGMENT_LENGTH) AS TOTAL_LENGTH, 
SUM(CASE WHEN t1.SURF_TYPE IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20) THEN t1.SEGMENT_LENGTH ELSE 0 END) AS ASPAL, 
SUM(CASE WHEN t1.SURF_TYPE IN (21) THEN t1.SEGMENT_LENGTH ELSE 0 END) AS RIGID, 
SUM(CASE WHEN t1.SURF_TYPE IN (1, 2) THEN t1.SEGMENT_LENGTH ELSE 0 END) AS TANAH,
{{var('year')}} as YEAR,
CURRENT_TIMESTAMP as UPDATE_DATE
FROM
( 
    SELECT LINKID, FROM_STA, TO_STA, SURF_TYPE, SEGMENT_LENGTH
    FROM {{ref("stg_rni_combined")}}
    {% if routes %} 
        WHERE LINKID IN (
            {%- for route in routes -%}
                '{{ route }}'{%- if not loop.last -%}, {%- endif -%}
            {%- endfor -%}
        )
    {%- else -%}
        WHERE 1=1
    {% endif %}
    AND year = {{var('year')}}
    AND semester = {{var('semester')}} 
) t1 
{{ get_prov_name('t1') }}
GROUP BY t1.LINKID
