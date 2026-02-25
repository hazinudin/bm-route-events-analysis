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
SUM(CASE WHEN t1.ROAD_TYPE IN (1, 6) THEN t1.SEGMENT_LENGTH ELSE 0 END) AS ROAD_TYPE_1, 
SUM(CASE WHEN t1.ROAD_TYPE IN (2, 7) THEN t1.SEGMENT_LENGTH ELSE 0 END) AS ROAD_TYPE_2, 
SUM(CASE WHEN t1.ROAD_TYPE IN (3, 8, 9, 11, 14, 16, 18, 23, 24, 25) THEN t1.SEGMENT_LENGTH ELSE 0 END) AS ROAD_TYPE_3, 
SUM(CASE WHEN t1.ROAD_TYPE IN (4, 10, 12, 13, 22, 26) THEN t1.SEGMENT_LENGTH ELSE 0 END) AS ROAD_TYPE_4, 
SUM(CASE WHEN t1.ROAD_TYPE IN (5, 15, 17, 19, 20, 21) THEN t1.SEGMENT_LENGTH ELSE 0 END) AS ROAD_TYPE_5,
{{var('year')}} as YEAR,
CURRENT_TIMESTAMP as UPDATE_DATE
FROM
( 
    SELECT LINKID, FROM_STA, MAX(TO_STA) AS TO_STA, MAX(ROAD_TYPE) AS ROAD_TYPE, MAX(SEGMENT_LENGTH) AS SEGMENT_LENGTH
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
    GROUP BY LINKID, FROM_STA
) t1 
{{ get_prov_name('t1') }}
GROUP BY t1.LINKID
