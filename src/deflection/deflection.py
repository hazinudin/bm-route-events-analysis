"""
This script contains function used to calculate FWD/LWD d0-d200 and its normalized value. The calculation also includes
sorting process and table lookup from a database connection.
"""
import pandas as pd
import numpy as np
from typing import Literal
from sqlalchemy import Engine


class Deflection(object):
    def __init__(
            self, 
            df: pd.DataFrame, 
            force_col: str, 
            data_type: Literal['FWD', 'LWD', 'BB'], 
            d0_col: str, 
            d200_col: str, 
            asp_temp: str, 
            routeid_col:str ='LINKID', 
            from_m_col:str ='FROM_STA',
            to_m_col:str ='TO_STA', 
            survey_direc:str ='SURVEY_DIREC', 
            surf_thickness_col:str ='SURF_THICKNESS', 
            force_ref:int =40,
            routes='ALL', 
            sort_only:bool =False, 
            sta_col:str =None, 
            conn: Engine = None,
            **kwargs
        ):
        """
        This class is used to calculate D0-D200 value for FWD/LWD
        :param df: The input Pandas DataFrame
        :param d0_col: The D0 column.
        :param d200_col: The d200 column.
        :param asp_temp: The asphalt temperature column.
        :param routeid_col: The route id column
        :param from_m_col: The From Measure column
        :param to_m_col: The To Measure column
        :param survey_direc: The Survey Direction column
        :param surf_thickness_col: The survey thickness column.
        :param force_col: The Force/Load column
        :param data_type: FWD or LWD data set
        :param force_ref: The value of reference force in kN.
        :param sta_col: STA column for Non FROM-TO data.
        """
        self.conn:Engine = conn

        if data_type == 'FWD' and (survey_direc is None):
            raise ValueError("Type is FWD but survey_direc is None")

        if routes == 'ALL':
            self.df = df.copy(deep=True)
        elif type(routes) == list:
            self.df = df.loc[df[routeid_col].isin(routes)].copy(deep=True)
        else:
            self.df = df.loc[df[routeid_col] == routes].copy(deep=True)

        self.force_col = force_col
        self.route_col = routeid_col
        self.from_m = from_m_col
        self.to_m = to_m_col
        self.sta_m = sta_col
        self.force_ref = force_ref
        self.survey_direc = survey_direc
        self.d0_col = d0_col
        self.d200_col = d200_col
        self.surf_thickness_col = surf_thickness_col

        if data_type != "BB":
            self.norm_d0 = 'NORM_'+self.d0_col
            self.norm_d200 = 'NORM_'+self.d200_col
            self.corr_d0 = 'CORR_'+self.d0_col
            self.corr_d200 = 'CORR_'+self.d200_col
            self.curvature = 'D0_D200'
            self.corr_curvature = 'CORR_D0_D200'

        self.data_type = data_type
        self.sort_only = sort_only
        self.asp_temp = asp_temp

        # BB columns.
        self.bb_df3_col = "BB_D3"
        self.bb_df2_col = "BB_D2"
        self.bb_df1_col = "BB_D1"
        self.bb_dmax_col = "BB_D0"
        self.bb_curvature = "BB_D0_D200"
        self.bb_dmax_corr_col = "BB_D0_CORR"
        self.bb_curvature_corr = "BB_D0_D200_CORR"

        if ((self.from_m is None) or (self.to_m is None)) and (self.sta_m is None):
            raise ValueError("from_m or to_m is None and sta_m column is also None.")

        if self.data_type == 'FWD':
            self.sorted: pd.DataFrame | None = self._sorting()
        elif (self.data_type == 'LWD') or (self.data_type == 'BB'):  # For LWD and BB there is no sorting required.
            self.sorted: pd.DataFrame | None = self.df

        self.ampt_tlap = 41 / abs(self.sorted[asp_temp])  # The AMPT/TLAP series.

        if self.sorted is not None and (not sort_only):
            if (self.data_type == 'LWD') or (self.data_type == 'FWD'):
                self._normalized_d0_d200()  # Create and fill the normalized columns
                self.sorted[self.curvature] = self.sorted[self.norm_d0]-self.sorted[self.norm_d200]  # The d0-d200 columns
                self.get_temp_correction_df('D200_TEMP_CORRECTION', self.norm_d200, self.corr_d200)
                self.get_temp_correction_df('D0_TEMP_CORRECTION', self.norm_d0, self.corr_d0)
                self.sorted[self.corr_curvature] = self.sorted[self.corr_d0]-self.sorted[self.corr_d200]
            else:  # For BB data type.
                self.sorted[self.bb_dmax_col] = self.sorted[self.bb_df3_col]-self.sorted[self.bb_df1_col]
                self.sorted[self.bb_curvature] = self.sorted[self.bb_df2_col]-self.sorted[self.bb_df1_col]

                # Temperature correction
                self.get_temp_correction_df("BB_D0_TEMP_CORRECTION", self.bb_dmax_col, self.bb_dmax_corr_col)
                self.get_temp_correction_df("BB_D0_D200_TEMP_CORRECTION", self.bb_curvature, self.bb_curvature_corr)

                # BB to FWD conversion
                self.bb_fwd_conversion('BB_D0_FWD_CONVERSION', self.bb_dmax_corr_col, "FWD_D0")
                self.bb_fwd_conversion('BB_D0_D200_FWD_CONVERSION', self.bb_curvature_corr, "FWD_D0_D200")

            self.sorted.drop(['OBJECTID', 'SURVEY_DATE', 'UPDATE_DATE'], axis=1, inplace=True)

    def _sorting(self):
        """
        This class method sort the input DataFrame to get the closest row to referenced force value.
        :return:
        """
        if np.all(self.df[self.force_col].isnull()): # If all the row in Force column is Null.
            return None

        self.df['_ref_diff'] = self.df[self.force_col]-self.force_ref  # Create the diff column
        self.df['_ref_diff'] = self.df['_ref_diff'].abs()  # Get only the absolute value

        if (self.from_m is None) or (self.to_m is None):
            grouped = self.df.groupby([self.route_col, self.sta_m, self.survey_direc])
        else:
            grouped = self.df.groupby([self.route_col, self.survey_direc, self.from_m, self.to_m])  # Do a group by

        closest_index = grouped['_ref_diff'].idxmin()  # Get the closest index
        closest_row = self.df.loc[closest_index].drop('_ref_diff', axis=1)  # Get the closest rows

        return closest_row.reset_index(drop=True)  # Return closest row

    def _normalized_d0_d200(self):
        """
        This class method calculate the normalized value of D0 and D200 column.
        :return:
        """
        result = self.sorted[[self.d0_col, self.d200_col, self.force_col]].\
                 apply(lambda x: (self.force_ref/x[self.force_col])*(x/1000), axis=1)  # The normalized calculation

        self.sorted[[self.norm_d0, self.norm_d200]] = result[[self.d0_col, self.d200_col]]
        return self

    def get_temp_correction_df(self, lookup_table_path, deflection_col, corrected_col):
        """
        This class method calculate the temperature corrected value of D0 and D200.
        """
        lookup_df = pd.read_sql("SELECT * FROM {0}".format(lookup_table_path), con=self.conn).rename(columns=str.upper)

        if 'OBJECTID' in lookup_df.columns:
            lookup_df = lookup_df.drop('OBJECTID', axis=1)

        lookup_df.set_index("AMPT_TLAP", inplace=True)
        lookup_df.index = lookup_df.index.map(float)
        lookup_df.columns = lookup_df.columns.str.replace("TH", "")
        lookup_df.columns = lookup_df.columns.map(int)  # Change the column from string to integer.
        lookup_thickness = list(lookup_df)  # Available thickness from the lookup table columns.
        lookup_melt = pd.melt(lookup_df, ignore_index=False).reset_index()
        lookup_melt['AMPT_TLAP'] = pd.to_numeric(lookup_melt['AMPT_TLAP']*10).round(1).astype(int) # Important for join, float join sucks big time
        
        input_df = pd.concat(
            [
                self.ampt_tlap.apply(lambda x: int(np.round(x, 1)*10) if np.round(x, 1) < 1.8 else 1.8*10).astype(int),
                self.sorted[self.surf_thickness_col].apply(
                    lambda x: lookup_thickness[
                        np.argmin([abs(_ - x) for _ in lookup_thickness])
                    ]
                )
            ], 
            axis=1
        )

        temp_factor = input_df.merge(
            lookup_melt,
            left_on=['ASPHALT_TEMP', 'SURF_THICKNESS'],
            right_on=['AMPT_TLAP', 'variable'],
            how='left'
        )['value']

        self.sorted[corrected_col] = self.sorted[deflection_col]*temp_factor

        return self

    def bb_fwd_conversion(self, lookup_table, input_column, output_column):
        lookup_df = pd.read_sql("SELECT * FROM {0}".format(lookup_table), con=self.conn).rename(columns=str.upper)

        if 'OBJECTID' in lookup_df.columns:
            lookup_df = lookup_df.drop('OBJECTID', axis=1)

        lookup_thickness = lookup_df['SURF_THICKNESS'].tolist()
        lookup_factor = lookup_df['FACTOR'].tolist()

        coversion_factor = self.sorted[self.surf_thickness_col].apply(lambda x: lookup_factor[
                           np.argmin([abs(_ - x) for _ in lookup_thickness])])

        self.sorted[output_column] = self.sorted[input_column]*coversion_factor

        return self
