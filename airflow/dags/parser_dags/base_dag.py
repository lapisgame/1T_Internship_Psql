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
    """
    This class represents a base implementation of DAGs (Directed Acyclic Graphs).
    It provides methods for updating dictionaries, archiving data, running a model, and initializing and updating the DML core.

    Attributes:
        df (pandas.DataFrame): The main DataFrame.
        dfs (pandas.DataFrame): Additional DataFrames.

    Methods:
        update_dicts(): Updates the dictionaries using the DataManager class.
        archiving(data_to_closed): Archives the specified data using the DataManager class.
        model(df): Runs the model by performing data preprocessing using the DataPreprocessing class.
        dml_core_init(dfs): Initializes the DML core by loading the specified DataFrames using the DataManager class.
        dml_core_update(dfs): Updates the DML core with the specified DataFrames using the DataManager class.
    """

    def __init__(self):
        self.df = pd.DataFrame()
        self.dfs = pd.DataFrame()

    def update_dicts(self):
        """
        Updates the dictionaries using the DataManager class.
        """
        manager = DataManager(conn, engine, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        manager.work_with_static_dicts()

    def archiving(self, data_to_closed):
        """
        Archives the specified data using the DataManager class.

        Args:
            data_to_closed: The data to be archived.
        """
        manager = DataManager(conn, engine, pd.DataFrame(), data_to_closed, pd.DataFrame())
        manager.delete_and_archive()

    def model(self, df):
        """
        Runs the model by performing data preprocessing using the DataPreprocessing class.

        Args:
            df: The DataFrame to be processed.
        """
        test = DataPreprocessing(df)
        test.call_all_functions()
        self.dfs = test.dict_all_data

    def dml_core_init(self, dfs):
        """
        Initializes the DML core by loading the specified DataFrames using the DataManager class.

        Args:
            dfs: The DataFrames to be loaded into the DML core.
        """
        manager = DataManager(conn, engine, dfs, pd.DataFrame(), pd.DataFrame())
        manager.init_load()

    def dml_core_update(self, dfs):
        """
        Updates the DML core with the specified DataFrames using the DataManager class.

        Args:
            dfs: The DataFrames to be loaded and updated in the DML core.
        """
        manager = DataManager(conn, engine, dfs, pd.DataFrame(), pd.DataFrame())
        manager.load_and_update_actual_data()



