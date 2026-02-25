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
t2.LINKID,
MAX({{ prov_name_column() }}) as BM_PROV_ID, 
SUM(t2.SEGMENT_LENGTH) AS TOTAL_LENGTH, 
SUM(t2.LANE_WIDTH*t2.SEGMENT_LENGTH)/SUM(t2.SEGMENT_LENGTH) AS LANE_WIDTH, 
SUM(CASE WHEN t2.LANE_WIDTH <= 4.5 THEN t2.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_1, 
SUM(CASE WHEN t2.LANE_WIDTH > 4.5 AND t2.LANE_WIDTH <= 6 THEN t2.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_2, 
SUM(CASE WHEN t2.LANE_WIDTH > 6 AND t2.LANE_WIDTH <= 7 THEN t2.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_3, 
SUM(CASE WHEN t2.LANE_WIDTH > 7 AND t2.LANE_WIDTH <= 8 THEN t2.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_4, 
SUM(CASE WHEN t2.LANE_WIDTH > 8 AND t2.LANE_WIDTH <= 14 THEN t2.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_5, 
SUM(CASE WHEN t2.LANE_WIDTH > 14 THEN t2.SEGMENT_LENGTH ELSE 0 END) AS WIDTH_CAT_6,
SUM(CASE WHEN t2.SURF_WIDTH < 7 THEN t2.SEGMENT_LENGTH ELSE 0 END) AS SUB_STD_WIDTH,
SUM(CASE WHEN t2.SURF_WIDTH >= 7 THEN t2.SEGMENT_LENGTH ELSE 0 END) AS STD_WIDTH,
{{var('year')}} as YEAR,
CURRENT_TIMESTAMP as UPDATE_DATE
FROM
( 
    SELECT 
        LINKID, 
        FROM_STA, 
        MAX(TO_STA) AS TO_STA, 
        SUM(LANE_WIDTH) AS LANE_WIDTH, 
        MAX(SEGMENT_LENGTH) AS SEGMENT_LENGTH,
        CASE 
            WHEN MAX(MED_WIDTH) = 0 OR MAX(MED_WIDTH) IS NULL 
            THEN MAX(SURF_WIDTH) 
            ELSE SUM(SURF_WIDTH) 
        END AS SURF_WIDTH
    FROM (
        SELECT 
            LINKID, 
            FROM_STA, 
            TO_STA,
            substr(LANE_CODE, 1, 1) as lane_side,
            MAX(SEGMENT_LENGTH) AS SEGMENT_LENGTH,
            SUM(LANE_WIDTH) AS LANE_WIDTH,
            MAX(SURF_WIDTH) AS SURF_WIDTH,
            MAX(MED_WIDTH) AS MED_WIDTH
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
        GROUP BY LINKID, FROM_STA, TO_STA, substr(LANE_CODE, 1, 1)
    ) t0
    GROUP BY LINKID, FROM_STA
) t2 
{{ get_prov_name('t2') }}
GROUP BY t2.LINKID
