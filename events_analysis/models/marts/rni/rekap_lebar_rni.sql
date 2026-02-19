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
SUM(t1.SEGMENT_LENGTH) AS TOTAL_LENGTH, 
SUM(t1.LANE_WIDTH*t1.SEGMENT_LENGTH)/SUM(t1.SEGMENT_LENGTH) AS LANE_WIDTH, 
SUM(CASE WHEN t1.LANE_WIDTH <= 4.5 THEN t1.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_1, 
SUM(CASE WHEN t1.LANE_WIDTH > 4.5 AND t1.LANE_WIDTH <= 6 THEN t1.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_2, 
SUM(CASE WHEN t1.LANE_WIDTH > 6 AND t1.LANE_WIDTH <= 7 THEN t1.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_3, 
SUM(CASE WHEN t1.LANE_WIDTH > 7 AND t1.LANE_WIDTH <= 8 THEN t1.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_4, 
SUM(CASE WHEN t1.LANE_WIDTH > 8 AND t1.LANE_WIDTH <= 14 THEN t1.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_5, 
SUM(CASE WHEN t1.LANE_WIDTH > 14 THEN t1.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_6,
{{var('year')}} as YEAR,
CURRENT_TIMESTAMP as UPDATE_DATE
FROM
( 
    SELECT LINKID, FROM_STA, MAX(TO_STA) AS TO_STA, SUM(LANE_WIDTH) AS LANE_WIDTH, MAX(SEGMENT_LENGTH) AS SEGMENT_LENGTH
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
GROUP BY t1.LINKID
