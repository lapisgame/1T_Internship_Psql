import json
from raw.connect_settings import conn, engine
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import logging as log
from logging import handlers
from airflow.models import Variable
from datetime import datetime, timedelta
import time
from airflow.utils.log.logging_mixin import LoggingMixin
import os
from sqlalchemy import create_engine
from core.ddl_core import DatabaseManager
from core.dml_core import DataManager
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.habr_career import HabrJobParser, table_name
from raw.variables_settings import variables, base_habr
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
        conn.autocommit = False

    def update_dicts(self):
        manager = DataManager(conn, engine, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        manager.work_with_static_dicts()

    def model(self, df):
        test = DataPreprocessing(df)
        test.call_all_functions()
        self.dfs = test.dict_all_data

    def dml_core_init(self, dfs):
        manager = DataManager(conn, engine, dfs, pd.DataFrame(), pd.DataFrame())
        manager.init_load()

    def dml_core_update_and_archivate(self, dfs, data_to_closed):
        manager = DataManager(conn, engine, dfs, data_to_closed, pd.DataFrame())
        manager.delete_archive_core_data()
        manager.load_ad_update_actual_data()


