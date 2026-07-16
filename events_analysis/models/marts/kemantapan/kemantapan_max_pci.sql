{{
    config(
        materialized='incremental',
        unique_key=['LINKID', 'YEAR'],
        incremental_strategy='delete+insert'
    )
}}

{% set rules = var('kemantapan_pci_ranges') %}

SELECT
    sde.gdb_util.next_rowid({{"'"+this.schema+"'"}}, {{"'"+this.name+"'"}}) as OBJECTID,
    CURRENT_TIMESTAMP as UPDATE_DATE,
    base.LINKID,
    {{ prov_name_column('base') }} as BM_PROV_ID,
    base.YEAR,
    base.PCI,
    base.TOTAL_LENGTH,
    {{ satker_balai_columns() }},

    {{kemantapan_columns()}}

FROM
(
    SELECT
        merged.LINKID as LINKID,

        {{var('year')}} as YEAR,
        SUM(SEGMENT_LENGTH) as TOTAL_LENGTH,
        MAX(merged.PCI) as PCI,

        {{ km_sum('merged', rules.paved.surf_types, rules.paved.ranges.good.min, none, 'P_GOOD_KM', none, 'PCI') }},
        {{ km_sum('merged', rules.paved.surf_types, rules.paved.ranges.fair.min, rules.paved.ranges.fair.max, 'P_FAIR_KM', none, 'PCI') }},
        {{ km_sum('merged', rules.paved.surf_types, rules.paved.ranges.poor.min, rules.paved.ranges.poor.max, 'P_POOR_KM', none, 'PCI') }},
        {{ km_sum('merged', rules.paved.surf_types, none, rules.paved.ranges.bad.max, 'P_BAD_KM', none, 'PCI') }},

        {{ km_sum('merged', rules.unpaved.surf_types, rules.unpaved.ranges.good.min, none, 'UP_GOOD_KM', none, 'PCI') }},
        {{ km_sum('merged', rules.unpaved.surf_types, rules.unpaved.ranges.fair.min, rules.unpaved.ranges.fair.max, 'UP_FAIR_KM', none, 'PCI') }},
        {{ km_sum('merged', rules.unpaved.surf_types, rules.unpaved.ranges.poor.min, rules.unpaved.ranges.poor.max, 'UP_POOR_KM', none, 'PCI') }},
        {{ km_sum('merged', rules.unpaved.surf_types, none, rules.unpaved.ranges.bad.max, 'UP_BAD_KM', none, 'PCI') }}

    FROM (
        select
            e.LINKID,
            AVG(e.PCI) as PCI,
            max(e.SURF_TYPE) as SURF_TYPE,
            max(e.SEGMENT_LENGTH) as SEGMENT_LENGTH

        from ({{ rni_pci_join(var('semester'), var('year'), var('routes', none)) }}) e
        GROUP BY e.LINKID, e.FROM_STA, e.TO_STA
    ) merged

    GROUP BY merged.LINKID
) base
{# {{ join_satker_balai('base') }} #}
{{ get_prov_name('base') }}
