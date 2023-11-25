import time
import pandas as pd
from selenium import webdriver
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import sys
import numpy as np
from airflow.utils.log.logging_mixin import LoggingMixin
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.variables_settings import variables

raw_tables = variables['raw_tables']
# table_name = variables['raw_tables'][0]['raw_tables_name']
schemes = variables["schemes"]

class BaseJobParser:
    """
    Base class for parsing job vacancies
    """
    def __init__(self, url, profs, conn):
        self.log = LoggingMixin().log
        columns = [
            'vacancy_url', 'vacancy_name', 'towns', 'level', 'company', 'salary_from',
            'salary_to', 'exp_from', 'exp_to', 'description', 'job_type', 'job_format',
            'languages', 'skills', 'source_vac', 'date_created', 'date_of_download',
            'status', 'date_closed', 'version_vac', 'actual'
        ]
        self.df = pd.DataFrame(columns=columns)
        self.dataframe_to_closed = pd.DataFrame(columns=columns)
        self.dataframe_to_update = pd.DataFrame(columns=columns)
        self.log.info("Созданы DataFrame's для записи вакансий")

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        self.browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
        # self.browser = webdriver.Chrome(options=options)
        self.url = url
        self.browser.get(self.url)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.profs = profs
        self.schema = schemes['raw']
        self.raw_tables = raw_tables
        self.conn = conn
        self.cur = conn.cursor()
    def scroll_down_page(self, page_height=0):
        """
        Method for scrolling down the page
        """
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_page_height = self.browser.execute_script('return document.body.scrollHeight')
        if new_page_height > page_height:
            self.scroll_down_page(new_page_height)

    def stop(self):
        """
        Method to exit Selenium Webdriver
        """
        self.browser.quit()

    def find_vacancies(self):
        """
        Method for parsing vacancies, should be overridden in subclasses
        """
        raise NotImplementedError("You must define the find_vacancies method")

    def addapt_numpy_null(self):
        """
        This method registers adapters for NumPy float64 and int64 data types in PostgreSQL.
        It creates two adapter functions, `adapt_numpy_float64` and `adapt_numpy_int64`, which return the input
        values as is.
        This ensures that NumPy float64 and int64 values are adapted correctly when inserted into PostgreSQL.
        The adapters are then registered using the `register_adapter` function from the `psycopg2.extensions` module.
        Method also replaces any missing values (NaN) in the DataFrame `self.df`
        with the string 'NULL'. This is done using the "fillna()" method, where we pass
        "psycopg2.extensions.AsIs('NULL')" as the value to fill missing values.
        """
        self.cur = self.conn.cursor()

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        self.df = self.df.fillna(psycopg2.extensions.AsIs('NULL'))
        self.dataframe_to_closed = self.dataframe_to_closed.fillna(psycopg2.extensions.AsIs('NULL'))
        self.dataframe_to_update = self.dataframe_to_update.fillna(psycopg2.extensions.AsIs('NULL'))

    def save_df(self):
        """
        Method for saving data from pandas DataFrame
        """
        raise NotImplementedError("You must define the save_df method")

    def generating_dataframes(self):
        """
        Method for generating dataframes for data updates
        """

    def update_database_queries(self):
        """
        Method for performing data update in the database
        """



