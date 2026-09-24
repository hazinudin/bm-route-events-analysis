"""
AADTPipeline: the main orchestrator.

This is the only class a caller needs to interact with.  It coordinates
repositories, SQL builders, and pure calculators to reproduce the exact
legacy AADT/VCR/Capacity output.
"""

import os
import pandas as pd
import numpy as np
from .smd_config import SMDConfigs
from .latest_table import LatestTable
from .rni_summary import road_type_group_df as _rtg_df
from . import config
from .db_context import DBContext
from . import queries
from . import calculators


class AADTPipeline:
    """
    End-to-end AADT calculator that mirrors ``KemantapanService``
    (``data_type='AADT'``) without mutating any ``SMD_Package`` files.

    Usage
    -----
    >>> pipe = AADTPipeline()
    >>> result = pipe.calculate(routes=['11010001', '11010002'], year=2021)
    >>> result.columns
    Index(['LINKID', 'VCR', 'VOLUME', 'CAPACITY',
           'NUM_VEH1', 'NUM_VEH2', ..., 'CESA', 'AADT'], dtype='object')
    """

    def __init__(self, connection=None, smd_config=None):
        """
        Parameters
        ----------
        connection : cx_Oracle.Connection, optional
            Oracle connection.  Defaults to ``SMD_Package.db_conn.smd_connection``.
        smd_config : SMDConfigs, optional
            Config object.  Fresh instance created if omitted.
        """
        self.db = DBContext(connection)
        self.smd_config = smd_config or SMDConfigs()

        # Column names pulled from SMDConfigs (same as legacy classes)
        rni = self.smd_config.table_fields['rni']
        self.rni_routeid = rni['route_id']
        self.rni_from_m = rni['from_measure']
        self.rni_to_m = rni['to_measure']
        self.rni_lane_code = rni['lane_code']
        self.rni_road_type = rni['road_type']
        self.rni_lane_width = rni['lane_width']
        self.rni_left_terr = rni['left_terrain_col']
        self.rni_right_terr = rni['right_terrain_col']
        self.rni_segment_len = rni['length_col']
        self.rni_li_sh_w = rni.get('left_inner_sh_w', 'LEFT_INNER_SH_W')
        self.rni_lo_sh_w = rni.get('left_outer_sh_w', 'LEFT_OUTER_SH_W')
        self.rni_ri_sh_w = rni.get('right_inner_sh_w', 'RIGHT_INNER_SH_W')
        self.rni_ro_sh_w = rni.get('right_outer_sh_w', 'RIGHT_OUTER_SH_W')

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def calculate(self, routes, year, semester=None):
        """
        Calculate AADT / VCR / Capacity for the requested routes.

        Parameters
        ----------
        routes : list[str] or str
            Route ID(s), or the string ``'ALL'``.
        year : int
            Data year.
        semester : int or None, optional
            If provided, forces ``RNI_{semester}_{year}``; otherwise the
            latest RNI table for that year is discovered automatically.

        Returns
        -------
        pd.DataFrame
        """
        # 1. Resolve RNI table name
        if semester is not None:
            rni_table = config.RNI_TABLE_FMT.format(semester=semester, year=year)
        else:
            rni_table = LatestTable(year, 'RNI').latest_table

        # 2. Populate temp route table once (shared by all queries)
        self.db.insert_temp_routes(routes)

        # =====================================================================
        # Step A – Post-class AADT (vehicle columns + CESA + AADT sum)
        # =====================================================================
        aadt_raw_sql = queries.post_class_aadt_sql(
            routes=routes,
            year=year,
            connection=self.db.connection,
            routeid_col=self.rni_routeid,
            temp_route_table=config.TEMP_ROUTE_TABLE,
        )
        aadt_raw = self.db.read_sql(aadt_raw_sql)
        aadt_raw = aadt_raw.set_index(self.rni_routeid)

        # Add CESA  (pure calc)
        aadt_df = calculators.add_cesa(aadt_raw)
        # Add AADT sum  (pure calc)
        aadt_df = calculators.add_aadt_sum(aadt_df)
        aadt_df = aadt_df.reset_index()

        # =====================================================================
        # Step B – Traffic Volume (FLOW → PCE → PCEH)
        # =====================================================================
        volume_df = self._calculate_volume(routes, year, aadt_df, rni_table)

        # =====================================================================
        # Step C – Road Capacity (weighted average per route)
        # =====================================================================
        capacity_df = self._calculate_capacity(routes, year, aadt_raw_sql, rni_table)

        # =====================================================================
        # Step D – Final VCR merge
        # =====================================================================
        veh_columns = [c for c in aadt_df.columns
                       if c.startswith(config.VEHICLE_COL_PREFIX)]
        veh_columns.sort()

        result = calculators.merge_vcr(
            volume_df=volume_df,
            capacity_df=capacity_df,
            aadt_df=aadt_df,
            routeid_col=self.rni_routeid,
            veh_columns=veh_columns,
        )

        # Commit temp-route inserts so they remain visible for the SQL
        # (legacy code relies on auto-commit / shared transaction)
        self.db.commit()

        return result

    # ------------------------------------------------------------------
    # Internal: Volume
    # ------------------------------------------------------------------
    def _calculate_volume(self, routes, year, aadt_df, rni_table):
        """
        Replicate ``TrafficVolume.calculate_volume()``:
        FLOW (per band) → PCE (interpolated) → PCEH → max per LINKID.
        """
        # -- 1. RNI aggregated data -----------------------------------------
        rni_sql = queries.get_rni_data_sql(
            routes=routes,
            rni_table=rni_table,
            routeid_col=self.rni_routeid,
            from_m_col=self.rni_from_m,
            to_m_col=self.rni_to_m,
            lane_width=self.rni_lane_width,
            left_terr=self.rni_left_terr,
            right_terr=self.rni_right_terr,
            road_type_col=self.rni_road_type,
        )
        rni_df = self.db.read_sql(rni_sql)

        # -- 2. Coefficient tables ------------------------------------------
        pce_coeff_df = self.db.read_sql(f"SELECT * FROM {config.PCE_TABLE}")
        pce_coeff_df['TERR'] = pce_coeff_df['TERR'].map(config.TERRAIN_MAP)

        all_pce_cols = np.array(pce_coeff_df.columns.tolist())
        pce_veh_cols = all_pce_cols[np.char.startswith(all_pce_cols, config.PCE_COL_PREFIX)]
        pce_veh_cols.sort()

        flowband_df = self.db.read_sql(f"SELECT * FROM {config.FLOWBAND_TABLE}")

        road_type_group_df = _rtg_df()

        # -- 3. Compute FLOW per band in-memory -----------------------------
        #    Legacy:  SELECT LINKID, SUM(NUM_VEH_i * HDA_i) AS FLOW, band AS BAND
        #             FROM (post_class_aadt_sql)
        #    We replicate the arithmetic using the already-fetched AADT df.
        flow_frames = []
        for _, fb_row in flowband_df.iterrows():
            band = fb_row['BAND']
            hda_lv = fb_row['HDA_LV']
            hda_hv = fb_row['HDA_HV']

            flow_vals = pd.Series(0.0, index=aadt_df.index)
            for vcol in config.ALL_VEHICLE_COLS:
                hda = hda_hv if vcol in config.HEAVY_VEHICLES else hda_lv
                flow_vals += aadt_df[vcol] * hda

            band_df = pd.DataFrame({
                self.rni_routeid: aadt_df[self.rni_routeid],
                'FLOW': flow_vals.values,
                'BAND': band,
            })
            flow_frames.append(band_df)

        all_band = pd.concat(flow_frames, ignore_index=True)

        # -- 4. Merge with RNI ----------------------------------------------
        all_band = all_band.merge(rni_df, on=self.rni_routeid)

        # -- 5. Interpolate PCE per row -------------------------------------
        def _pce_for_row(row):
            return calculators.interpolate_pce(
                flow=row['FLOW'],
                road_type=row[self.rni_road_type],
                terr=row['TERR'],
                width=row[self.rni_lane_width],
                pce_coeff_df=pce_coeff_df,
                road_type_group_df=road_type_group_df,
                pce_veh_cols=pce_veh_cols.tolist(),
            )

        all_band[pce_veh_cols.tolist()] = all_band.apply(_pce_for_row, axis=1)

        # -- 6. Compute PCEH per row ----------------------------------------
        all_band[config.PCEH_COL] = all_band.apply(
            lambda row: calculators.calculate_pceh_row(
                row=row,
                aadt_df=aadt_df,
                flowband_df=flowband_df,
                routeid_col=self.rni_routeid,
                all_veh_cols=config.ALL_VEHICLE_COLS,
                heavy_veh=config.HEAVY_VEHICLES,
                pce_veh_cols=pce_veh_cols.tolist(),
            ),
            axis=1,
        )

        # -- 7. Max PCEH per LINKID -----------------------------------------
        result = (
            all_band.groupby(self.rni_routeid)[config.PCEH_COL]
            .max()
            .reset_index()
        )
        return result

    # ------------------------------------------------------------------
    # Internal: Capacity
    # ------------------------------------------------------------------
    def _calculate_capacity(self, routes, year, aadt_raw_sql, rni_table):
        """
        Replicate ``RoadCapacity.calculate_capacity()`` using the same
        nested SQL but built explicitly here.
        """
        seg_sql = queries.segment_capacity_sql(
            routes=routes,
            year=year,
            aadt_df_sql=aadt_raw_sql,
            rni_table=rni_table,
            temp_route_table=config.TEMP_ROUTE_TABLE,
            routeid_col=self.rni_routeid,
            from_m_col=self.rni_from_m,
            to_m_col=self.rni_to_m,
            segment_len_col=self.rni_segment_len,
            lane_width=self.rni_lane_width,
            road_type_col=self.rni_road_type,
            left_terr=self.rni_left_terr,
            right_terr=self.rni_right_terr,
            left_inner_sh_w=self.rni_li_sh_w,
            left_outer_sh_w=self.rni_lo_sh_w,
            right_inner_sh_w=self.rni_ri_sh_w,
            right_outer_sh_w=self.rni_ro_sh_w,
            veh8_col=config.VEH8_COL_DEFAULT,
        )

        cap_sql = queries.calculate_capacity_sql(
            segment_capacity_sql=seg_sql,
            routeid_col=self.rni_routeid,
            segment_len_col=self.rni_segment_len,
            year=year,
        )

        return self.db.read_sql(cap_sql)
