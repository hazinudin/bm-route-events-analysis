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
rni.BM_PROV_ID,
lrs.SK_LENGTH as TOTAL_LENGTH,
(ROAD_TYPE_1*(lrs.SK_LENGTH/TOTAL_LENGTH)) as ROAD_TYPE_1,
(ROAD_TYPE_2*(lrs.SK_LENGTH/TOTAL_LENGTH)) as ROAD_TYPE_2,
(ROAD_TYPE_3*(lrs.SK_LENGTH/TOTAL_LENGTH)) as ROAD_TYPE_3,
(ROAD_TYPE_4*(lrs.SK_LENGTH/TOTAL_LENGTH)) as ROAD_TYPE_4,
(ROAD_TYPE_5*(lrs.SK_LENGTH/TOTAL_LENGTH)) as ROAD_TYPE_5,
YEAR as YEAR,
CURRENT_TIMESTAMP as UPDATE_DATE    
from (select * from {{ref("rekap_tipe_jalan")}} where year = {{var('year')}}) rni
left join {{ref('active_lrs')}} lrs 
on rni.LINKID = lrs.LINKID
