{{
    config(
        materialized='incremental',
        unique_key=['LINKID', 'YEAR', 'SEMESTER'],
        incremental_strategy='delete+insert'
    )
}}

{% set rules = var('kemantapan_iri_ranges') %}

SELECT
    sde.gdb_util.next_rowid({{"'"+this.schema+"'"}}, {{"'"+this.name+"'"}}) as OBJECTID,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    base.LINKID,
    {{ prov_name_column('base') }} as BM_PROV_ID,
    base.YEAR,
    base.SEMESTER,
    base.IRI_POK,
    base.TOTAL_LENGTH,
    {{ satker_balai_columns() }},

    {{kemantapan_columns()}},
    IRI_MARGINAL_P,
    IRI_MARGINAL_UP

FROM
(
    SELECT
        merged.LINKID as LINKID,
        
        {{var('year')}} as YEAR,
        {{var('semester')}} as SEMESTER,
        SUM(SEGMENT_LENGTH) as TOTAL_LENGTH,
        AVG(merged.IRI_POK) as IRI_POK,

        {{ km_sum('merged', rules.paved.surf_types, none, rules.paved.ranges.good.max, 'P_GOOD_KM', none, 'IRI_POK') }},
        {{ km_sum('merged', rules.paved.surf_types, rules.paved.ranges.fair.min, rules.paved.ranges.fair.max, 'P_FAIR_KM', none, 'IRI_POK') }},
        {{ km_sum('merged', rules.paved.surf_types, rules.paved.ranges.poor.min, rules.paved.ranges.poor.max, 'P_POOR_KM', none, 'IRI_POK') }},
        {{ km_sum('merged', rules.paved.surf_types, rules.paved.ranges.bad.min, none, 'P_BAD_KM', none, 'IRI_POK') }},

        {{ km_sum('merged', rules.unpaved.surf_types, none, rules.unpaved.ranges.good.max, 'UP_GOOD_KM', none, 'IRI_POK') }},
        {{ km_sum('merged', rules.unpaved.surf_types, rules.unpaved.ranges.fair.min, rules.unpaved.ranges.fair.max, 'UP_FAIR_KM', none, 'IRI_POK') }},
        {{ km_sum('merged', rules.unpaved.surf_types, rules.unpaved.ranges.poor.min, rules.unpaved.ranges.poor.max, 'UP_POOR_KM', none, 'IRI_POK') }},
        {{ km_sum('merged', rules.unpaved.surf_types, rules.unpaved.ranges.bad.min, none, 'UP_BAD_KM', none, 'IRI_POK') }},

        {{ km_sum_inclusive('merged', rules.paved.surf_types, 6, 8, 'IRI_MARGINAL_P', none, 'IRI_POK') }},
        {{ km_sum_inclusive('merged', rules.unpaved.surf_types, 11, 12, 'IRI_MARGINAL_UP', none, 'IRI_POK') }}

    FROM (
        select 
            e.LINKID, 
            AVG(e.IRI_POK) as IRI_POK, 
            max(e.SURF_TYPE) as SURF_TYPE, 
            max(e.SEGMENT_LENGTH) as SEGMENT_LENGTH 
        
        from ({{ rni_iri_join(var('semester'), var('year'), var('routes', none)) }}) e
        GROUP BY e.LINKID, e.FROM_STA, e.TO_STA
    ) merged

    GROUP BY merged.LINKID
) base
{# {{ join_satker_balai('base') }} #}
{{ get_prov_name('base') }}