import unittest
from src.deflection.deflection import Deflection
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd


load_dotenv('tests/dev.env')
HOST = os.getenv('GDB_HOST')
USER = os.getenv('SMD_USER')
PWD = os.getenv('SMD_PWD')

engine = create_engine(f"oracle+oracledb://{USER}:{PWD}@{HOST}:1521/geodbbm")

class TestDeflection(unittest.TestCase):
    def test_fwd(self):
        """
        Docstring for test_calculate_deflection
        
        :param self: Description
        """
        # df = pd.read_sql("select * from FWD_2_2025 where linkid = '2803512'", con=engine).rename(columns=str.upper)
        df = pd.read_sql("select * from fwd_2_2025 where linkid not in (select linkid from fwd where year = 2025)", con=engine).rename(columns=str.upper)

        defl = Deflection(
            df=df,
            force_col='FORCE',
            data_type='FWD',
            d0_col='FWD_D1',
            d200_col='FWD_D2',
            asp_temp='ASPHALT_TEMP',
            from_m_col=None,
            to_m_col=None,
            sta_col='STA',
            conn=engine
        )

        self.assertFalse(defl.sorted.empty)
        self.assertFalse(defl.sorted['CORR_D0_D200'].isnull().any())
        self.assertTrue(True)