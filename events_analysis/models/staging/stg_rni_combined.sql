{{ config(
    materialized='incremental',
    unique_key=['LINKID', 'YEAR', 'SEMESTER'],
    incremental_strategy='delete+insert'
) }}

{%- set tables = [] -%}
{%- for year in [2022, 2023, 2024, 2025] -%}
    {%- for semester in [2] -%}
        {%- do tables.append({'year': year, 'semester': semester}) -%}
    {%- endfor -%}
{%- endfor -%}

with source_data as (

    {% if is_incremental() %}
        
        {%- set target_year = var('year', none) -%}
        {%- set target_semester = var('semester', none) -%}
        {%- set target_routes = var('routes', none) -%}

        {%- if target_year and target_semester -%}
            
            {%- set max_update_date_query -%}
                SELECT MAX(UPDATE_DATE) FROM {{ this }} 
                WHERE YEAR = {{ target_year }} AND SEMESTER = {{ target_semester }}
            {%- endset -%}

            {%- set max_update_date = run_query(max_update_date_query).columns[0][0] -%}

            SELECT 
                LINKID,
                FROM_STA,
                TO_STA,
                SEGMENT_LENGTH,
                SURF_TYPE,
                SURF_WIDTH,
                LANE_CODE,
                ROAD_TYPE,
                MED_WIDTH,
                LANE_WIDTH,
                UPDATE_DATE,
                {{ target_year }} as YEAR,
                {{ target_semester }} as SEMESTER
            FROM {{ source('binamarga', 'RNI_' ~ target_semester ~ '_' ~ target_year) }}
            WHERE 1=1
            {% if target_routes %}
                AND LINKID IN (
                    {%- for route in target_routes -%}
                        '{{ route }}'{%- if not loop.last -%}, {%- endif -%}
                    {%- endfor -%}
                )
            {%- endif -%}

        {%- else -%}

            -- If year/semester are missing during an incremental run, return an empty result set
            -- to prevent accidental processing of the entire dataset.
            SELECT 
                LINKID,
                FROM_STA,
                TO_STA,
                SEGMENT_LENGTH,
                SURF_TYPE,
                SURF_WIDTH,
                LANE_CODE,
                ROAD_TYPE,
                MED_WIDTH,
                LANE_WIDTH,
                UPDATE_DATE,
                2022 as YEAR,
                1 as SEMESTER
            FROM {{ source('binamarga', 'RNI_1_2022') }} 
            WHERE 1=0

        {%- endif -%}

    {% else %}

        -- Full Refresh: Union all defined tables
        {% for t in tables %}
            SELECT 
                LINKID,
                FROM_STA,
                TO_STA,
                SEGMENT_LENGTH,
                SURF_TYPE,
                SURF_WIDTH,
                LANE_CODE,
                ROAD_TYPE,
                MED_WIDTH,
                LANE_WIDTH,
                UPDATE_DATE,
                {{ t.year }} as YEAR,
                {{ t.semester }} as SEMESTER
            FROM {{ source('binamarga', 'RNI_' ~ t.semester ~ '_' ~ t.year) }}
            {% if not loop.last %} UNION ALL {% endif %}
        {%- endfor -%}

    {%- endif -%}

)

select * from source_data
