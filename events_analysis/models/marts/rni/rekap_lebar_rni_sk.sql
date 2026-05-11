{{
    config(
        materialized='incremental',
        unique_key=['LINKID', 'YEAR'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

select
sde.gdb_util.next_rowid({{"'"+this.schema+"'"}}, {{"'"+this.name+"'"}}) as OBJECTID,
rni.LINKID,
rni.BM_PROV_ID,
lrs.SK_LENGTH as TOTAL_LENGTH,
LANE_WIDTH,
(WIDTH_CAT_1*(lrs.SK_LENGTH/TOTAL_LENGTH)) as WIDTH_CAT_1,
(WIDTH_CAT_2*(lrs.SK_LENGTH/TOTAL_LENGTH)) as WIDTH_CAT_2,
(WIDTH_CAT_3*(lrs.SK_LENGTH/TOTAL_LENGTH)) as WIDTH_CAT_3,
(WIDTH_CAT_4*(lrs.SK_LENGTH/TOTAL_LENGTH)) as WIDTH_CAT_4,
(WIDTH_CAT_5*(lrs.SK_LENGTH/TOTAL_LENGTH)) as WIDTH_CAT_5,
(WIDTH_CAT_6*(lrs.SK_LENGTH/TOTAL_LENGTH)) as WIDTH_CAT_6,
(SUB_STD_WIDTH*(lrs.SK_LENGTH/TOTAL_LENGTH)) as SUB_STD_WIDTH,
(STD_WIDTH*(lrs.SK_LENGTH/TOTAL_LENGTH)) as STD_WIDTH,
YEAR as YEAR,
CURRENT_TIMESTAMP as UPDATE_DATE    
from (select * from {{ref("rekap_lebar_rni")}} where year = {{var('year')}}) rni
left join {{ref('active_lrs')}} lrs 
on rni.LINKID = lrs.LINKID