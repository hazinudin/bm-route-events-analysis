"""
Pure calculation functions.

Every function in this module works on plain ``pandas.DataFrame`` / ``Series``
objects and has **no database side effects**.  They are direct ports of the
legacy ``SMD_Package`` logic and must not be altered without a regression test.
"""

import os
import json
import numpy as np
import pandas as pd
from . import config


# ===========================================================================
# PCE interpolation  (TrafficVolume._find_pce)
# ===========================================================================
def interpolate_pce(flow, road_type, terr, width,
                    pce_coeff_df, road_type_group_df, pce_veh_cols):
    """
    Look up / interpolate PCE coefficients for a single (flow, road_type, terr, width).

    Parameters
    ----------
    flow : float
        Hourly flow (veh/h).
    road_type : int or float
        Raw ROAD_TYPE from RNI.
    terr : int or float
        Terrain code (1, 2, 3).
    width : float
        Lane width [m].
    pce_coeff_df : pd.DataFrame
        Rows from ``PCE_COEFFICIENT`` with columns ``['FLOW', 'TERR', 'ROAD_TYPE',
        'PCE_01', 'PCE_02', ...]``.
    road_type_group_df : pd.DataFrame
        Mapping ``ROAD_TYPE → ROAD_TYPE_GROUP``.
    pce_veh_cols : list[str]
        Ordered list of PCE columns (e.g. ``['PCE_01', 'PCE_02', ...]``).

    Returns
    -------
    pd.Series
        Interpolated PCE values for every vehicle class.
    """
    # 1. Map raw road type → road-type group
    type_group = (
        road_type_group_df
        .loc[road_type_group_df['ROAD_TYPE'] == road_type, 'ROAD_TYPE_GROUP']
        .astype(int)
        .iloc[0]
    )

    # 2. Filter coefficient table
    mask = (
        (pce_coeff_df['TERR'] == int(terr)) &
        (pce_coeff_df['ROAD_TYPE'] == int(type_group))
    )
    pce_df = pce_coeff_df.loc[mask].copy()

    # 3. Find bounding flow rows
    pce_df['_flow_diff'] = pce_df['FLOW'] - flow

    if flow > pce_df['FLOW'].max():
        flow = pce_df['FLOW'].max()
        above_id = pce_df['FLOW'].idxmax()
        sorted_idx = pce_df['FLOW'].sort_values().index
        below_id = sorted_idx[len(pce_df) - 2]
    else:
        below_id = pce_df.loc[pce_df['_flow_diff'] < 0, '_flow_diff'].idxmax()
        above_id = pce_df.loc[pce_df['_flow_diff'] > 0, '_flow_diff'].idxmin()

    selection = pce_df.loc[[below_id, above_id]].sort_values('FLOW')

    # 4. Linear interpolation per PCE column
    flow_below = pce_df.loc[below_id, 'FLOW']
    flow_above = pce_df.loc[above_id, 'FLOW']

    def _interp(col):
        return (
            col[below_id] +
            ((col[above_id] - col[below_id]) * (flow - flow_below)) /
            (flow_above - flow_below)
        )

    result = selection[pce_veh_cols].apply(_interp, axis=0)

    # 5. Width adjustment for light vehicles (PCE_01)
    if width < config.WIDTH_THRESHOLD_LOW:
        result['PCE_01'] = result['PCE_01'] * config.WIDTH_LT_6_FACTOR
    if width > config.WIDTH_THRESHOLD_HIGH:
        result['PCE_01'] = result['PCE_01'] * config.WIDTH_GT_8_FACTOR

    return result


# ===========================================================================
# PCEH (hourly passenger-car equivalent)  (TrafficVolume._calculate_pceh)
# ===========================================================================
def calculate_pceh_row(row, aadt_df, flowband_df,
                       routeid_col='LINKID',
                       all_veh_cols=config.ALL_VEHICLE_COLS,
                       heavy_veh=config.HEAVY_VEHICLES,
                       pce_veh_cols=None):
    """
    Compute PCEH for a single (route, band) row.

    Parameters
    ----------
    row : pd.Series
        Must contain ``routeid_col``, ``'BAND'``, and every column in
        *pce_veh_cols*.
    aadt_df : pd.DataFrame
        AADT table indexed by route (or with ``routeid_col`` as a column).
    flowband_df : pd.DataFrame
        Lookup table with columns ``['BAND', 'HDA_HV', 'HDA_LV']``.
    routeid_col : str
    all_veh_cols : list[str]
    heavy_veh : list[str]
    pce_veh_cols : list[str]
        Ordered PCE columns; if *None* they are auto-discovered from *row*.

    Returns
    -------
    float
        The summed PCEH value.
    """
    route = row[routeid_col]
    band = row['BAND']

    hda = flowband_df.loc[flowband_df['BAND'] == band, ['HDA_HV', 'HDA_LV']]

    if pce_veh_cols is None:
        pce_veh_cols = [c for c in row.index if c.startswith(config.PCE_COL_PREFIX)]
        pce_veh_cols.sort()

    pce = row[pce_veh_cols]

    # Pull the AADT row for this route
    if routeid_col in aadt_df.columns:
        aadt_slice = aadt_df.loc[aadt_df[routeid_col] == route, all_veh_cols]
    else:
        aadt_slice = aadt_df.loc[[route], all_veh_cols]

    aadt_slice = aadt_slice.reset_index(drop=True).T[0]

    veh_hda = aadt_slice.index.map(
        lambda x: hda['HDA_HV'].values[0] if x in heavy_veh else hda['HDA_LV'].values[0]
    )

    result = aadt_slice.values * pce.values * veh_hda.values
    return result.sum()


# ===========================================================================
# CESA  (TrafficSummary._add_cesa_col)
# ===========================================================================
def load_vdf():
    """Load the Vehicle Damage Factor JSON from the internal data folder."""
    module_dir = os.path.dirname(__file__)
    json_path = os.path.join(module_dir, 'data', 'vdf.json')
    with open(json_path) as f:
        vdf_dict = json.load(f)
    return pd.DataFrame.from_dict(vdf_dict, orient='index').rename(columns={0: "VDF"})


def add_cesa(aadt_df, vdf_df=None, r_value=config.R_VALUE):
    """
    Add a ``CESA`` column to an AADT DataFrame.

    Parameters
    ----------
    aadt_df : pd.DataFrame
        Index = route, Columns = vehicle-count columns (``NUM_VEH*``).
    vdf_df : pd.DataFrame, optional
        VDF lookup indexed by vehicle-column name.  Loaded from JSON if omitted.
    r_value : float
        Default ``50.54``.

    Returns
    -------
    pd.DataFrame
        *aadt_df* with an additional ``CESA`` column.
    """
    if vdf_df is None:
        vdf_df = load_vdf()

    # transpose → join VDF on vehicle-name index → multiply → transpose back
    vdf_calc = aadt_df.transpose().join(vdf_df).fillna(1)
    vdf_calc = vdf_calc.apply(lambda x: x * x['VDF'], axis=1)
    vdf_calc = vdf_calc.drop('VDF', axis=1).transpose()

    vdf_calc['CESA'] = vdf_calc.apply(lambda x: x.sum(), axis=1)
    vdf_calc['CESA'] = vdf_calc['CESA'] * config.CESA_MULTIPLIER * float(r_value / config.CESA_SCALE)

    return aadt_df.join(vdf_calc[['CESA']])


# ===========================================================================
# AADT sum  (TrafficSummary._add_aadt_col)
# ===========================================================================
def add_aadt_sum(aadt_df, exclude=config.AADT_SUM_EXCLUDE):
    """
    Add an ``AADT`` column that is the row-wise sum of every vehicle column
    **except** the ones listed in *exclude*.

    Parameters
    ----------
    aadt_df : pd.DataFrame
        Must contain vehicle columns.
    exclude : list[str]
        Vehicle columns to omit from the sum.

    Returns
    -------
    pd.DataFrame
        Copy with new ``AADT`` column.
    """
    veh_cols = [c for c in aadt_df.columns if c.startswith(config.VEHICLE_COL_PREFIX)]
    included = [c for c in veh_cols if c not in exclude]
    aadt_df = aadt_df.copy()
    aadt_df['AADT'] = aadt_df[included].sum(axis=1)
    return aadt_df


# ===========================================================================
# VCR merge & column selection  (VCR.calculate_aadt_vcr)
# ===========================================================================
def merge_vcr(volume_df, capacity_df, aadt_df,
              routeid_col='LINKID',
              veh_columns=None):
    """
    Merge Volume, Capacity and AADT into the final VCR table.

    Parameters
    ----------
    volume_df : pd.DataFrame
        Columns ``[LINKID, PCEH]``.
    capacity_df : pd.DataFrame
        Columns ``[LINKID, WEIGHTED_CAP, YEAR]``.
    aadt_df : pd.DataFrame
        Post-class AADT with vehicle columns + ``CESA`` + ``AADT``.
    routeid_col : str
    veh_columns : list[str], optional
        Vehicle column names in their desired output order.

    Returns
    -------
    pd.DataFrame
        Final table with columns
        ``[LINKID, VCR, VOLUME, CAPACITY, <veh_columns>, CESA, AADT]``.
    """
    merged = volume_df.merge(capacity_df, on=routeid_col)
    merged['VCR'] = merged['PCEH'] / merged['WEIGHTED_CAP']
    merged = merged.merge(aadt_df, on=routeid_col)

    merged.rename(columns={'PCEH': 'VOLUME', 'WEIGHTED_CAP': 'CAPACITY'}, inplace=True)

    if veh_columns is None:
        veh_columns = [c for c in merged.columns
                       if c.startswith(config.VEHICLE_COL_PREFIX)]
        veh_columns.sort()

    col_selection = (
        [routeid_col, 'VCR', 'VOLUME', 'CAPACITY'] +
        veh_columns +
        ['CESA', 'AADT']
    )

    # Preserve only columns that exist (defensive)
    col_selection = [c for c in col_selection if c in merged.columns]
    return merged[col_selection]
