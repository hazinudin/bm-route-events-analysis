{{
    config(
        materialized='incremental',
        unique_key=['LINKID', 'YEAR'],
        incremental_strategy='delete+insert'
    )
}}

select
sde.gdb_util.next_rowid({{"'"+this.schema+"'"}}, {{"'"+this.name+"'"}}) as OBJECTID,
rni.LINKID,
lrs.SK_LENGTH as TOTAL_LENGTH,
(ASPAL*(lrs.SK_LENGTH/TOTAL_LENGTH)) as ASPAL,
(RIGID*(lrs.SK_LENGTH/TOTAL_LENGTH)) as RIGID,
(TANAH*(lrs.SK_LENGTH/TOTAL_LENGTH)) as TANAH,
YEAR as YEAR,
CURRENT_TIMESTAMP as UPDATE_DATE    
from (select * from {{ref("rekap_tipe_perkerasan")}} where year = {{var('year')}}) rni
left join {{ref('active_lrs')}} lrs 
on rni.LINKID = lrs.LINKID
