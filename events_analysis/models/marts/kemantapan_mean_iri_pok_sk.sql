{{
    config(
        materialized='incremental',
        unique_key=['LINKID', 'YEAR', 'SEMESTER'],
        incremental_strategy='delete+insert'
    )
}}

SELECT
    sde.gdb_util.next_rowid({{"'"+this.schema+"'"}}, {{"'"+this.name+"'"}}) as OBJECTID,
    base.UPDATE_DATE,
    base.LINKID,
    SUBSTR(base.LINKID, 1, 2) as BM_PROV_ID,
    base.YEAR,
    base.SEMESTER,
    base.IRI_POK,
    lrs.SK_LENGTH as TOTAL_LENGTH,
    base.SATKER_PPK_ID,
    base.BALAI_ID,
    
    {{ kemantapan_sk_columns('base', 'lrs') }}

FROM {{ ref("kemantapan_mean_iri_pok") }} base
LEFT JOIN {{ ref("active_lrs") }} lrs ON base.LINKID = lrs.LINKID
WHERE base.YEAR = {{ var('year') }} AND base.SEMESTER = {{ var('semester') }}
{% if var('routes', none) %}
    AND base.LINKID IN (
        {%- for route in var('routes') -%}
            '{{ route }}'{%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
{% endif %}
