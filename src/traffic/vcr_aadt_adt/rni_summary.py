"""
Minimal standalone RNI summary helper (replaces SMD_Package.event_table.rni.summary.RNISummary).

Only the ``road_type_group_df`` property is replicated because that is the
only surface used by the AADT pipeline.
"""

import json
import os
import pandas as pd


def road_type_group_df():
    """
    Load the road-type-group mapping JSON and return it as a DataFrame.

    Returns
    -------
    pd.DataFrame
        Columns: ``ROAD_TYPE_GROUP`` (int), ``ROAD_TYPE`` (int)
    """
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    json_path = os.path.join(data_dir, 'roadtype_group.json')

    with open(json_path) as j_file:
        type_dict = json.load(j_file)

    df = pd.DataFrame.from_dict(type_dict, orient='index').stack().reset_index(level=0)
    df.rename(columns={'level_0': 'ROAD_TYPE_GROUP', 0: 'ROAD_TYPE'}, inplace=True)
    df['ROAD_TYPE'] = df['ROAD_TYPE'].astype(int)
    return df
