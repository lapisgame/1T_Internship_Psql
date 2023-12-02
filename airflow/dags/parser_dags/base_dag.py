from connect_settings import conn, engine
import psycopg2

from datetime import datetime, timedelta
from core.dml_core import DataManager
import pandas as pd

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.model_spacy import DataPreprocessing


# Default dag arguments
default_args = {
    "owner": "admin_1T",
    'start_date': datetime(2023, 11, 26),
    'retry_delay': timedelta(minutes=5),
}


class BaseDags:

    def __init__(self):
        self.df = pd.DataFrame()
        self.dfs = pd.DataFrame()

    def update_dicts(self):
        manager = DataManager(conn, engine, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        manager.work_with_static_dicts()

    def archiving(self, data_to_closed):
        manager = DataManager(conn, engine, pd.DataFrame(), data_to_closed, pd.DataFrame())
        manager.delete_and_archive()

    def model(self, df):
        test = DataPreprocessing(df)
        test.call_all_functions()
        self.dfs = test.dict_all_data

    def dml_core_init(self, dfs):
        manager = DataManager(conn, engine, dfs, pd.DataFrame(), pd.DataFrame())
        manager.init_load()

    def dml_core_update(self, dfs):
        manager = DataManager(conn, engine, dfs, pd.DataFrame(), pd.DataFrame())
        manager.load_and_update_actual_data()



