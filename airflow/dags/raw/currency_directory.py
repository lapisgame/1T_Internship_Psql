import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import sys
import requests
import numpy as np
from datetime import datetime
from psycopg2.extras import execute_values
import logging as log
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from variables_settings import variables, base_exchange_rates, currencies
from connect_settings import conn

schemes = variables["schemes"]


class CurrencyDirectory:
    """
    This class represents a currency directory and provides methods to obtain currency exchange rates
    and save them in a database table.

    Parameters:
    - conn: The connection object to the database.
    - log: The log object for logging messages.
    - base_exchange_rates: The base URL for obtaining exchange rates.
    - schemes: A dictionary containing schema information.
    - currencies: A list of currencies to obtain exchange rates for.

    Attributes:
    - conn: The connection object to the database.
    - cur: The database cursor.
    - url: The base URL for obtaining exchange rates.
    - exchange_rate: A Pandas DataFrame to store the exchange rates.
    - schema: The schema for the raw exchange rate data.
    - table_name: The name of the currency directory table.
    - log: The log object for logging messages.
    - currencies: A list of currencies to obtain exchange rates for.
    - columns: The columns of the exchange_rate DataFrame.

    Methods:
    - obtaining_currency: Obtains the currency exchange rates from the given URL and stores them in the DataFrame.
    - adapt_numpy_null: Registers adapters for NumPy float64 and int64 data types in PostgreSQL,
                        and replaces missing values in the DataFrame with 'NULL'.
    - save_exchange_rate: Saves the exchange rates in the database table.
    """

    def __init__(self, conn, log, base_exchange_rates, schemes, currencies):
        self.conn = conn
        self.cur = conn.cursor()
        self.url = base_exchange_rates
        self.schema = schemes['raw']
        self.table_name = 'currency_directory'
        self.log = log
        self.currencies = currencies

        self.columns = ['exchange_rate_date', 'usd_rate', 'eur_rate', 'kzt_rate']
        self.exchange_rate = pd.DataFrame(columns=self.columns)

    def obtaining_currency(self):
        """
        Obtains the currency exchange rates from the given URL and stores them in the DataFrame.

        Uses the `requests` library to make a GET request to the `base_exchange_rates` URL.
        The response is expected to be in JSON format, and the exchange rates are extracted
        and stored in the `exchange_rate` DataFrame.
        """
        try:
            data = requests.get(self.url).json()
            self.exchange_rate = pd.concat(
                [self.exchange_rate, pd.DataFrame({'exchange_rate_date': [datetime.now().date()]})], ignore_index=True)
            for currency in self.currencies:
                value = float(data['Valute'][currency]['Value'])
                if currency == 'KZT':
                    value = value / 100
                value = round(value, 4)
                self.exchange_rate.at[0, currency.lower() + '_rate'] = value

        except Exception as e:
            self.log.error(f"An error occurred while obtaining currency exchange rates: {e}")
            raise

        # self.exchange_rate.reset_index(drop=True, inplace=True)

    def adapt_numpy_null(self):
        """
        Registers adapters for NumPy float64 and int64 data types in PostgreSQL.
        Creates two adapter functions, `adapt_numpy_float64` and `adapt_numpy_int64`,
        which return the input values as is.
        Ensures that NumPy float64 and int64 values are adapted correctly when inserted into PostgreSQL.
        Registers the adapters using the `register_adapter` function from the `psycopg2.extensions` module.
        Replaces any missing values (NaN) in the DataFrame `self.exchange_rate`
        with the string 'NULL' using the "fillna()" method and passing
        "psycopg2.extensions.AsIs('NULL')" as the value to fill missing values.
        """
        def adapt_numpy_null_values(value):
            return AsIs(value)

        register_adapter(np.float64, adapt_numpy_null_values)
        register_adapter(np.int64, adapt_numpy_null_values)

        self.exchange_rate = self.exchange_rate.fillna(psycopg2.extensions.AsIs('NULL'))

    def save_exchange_rate(self):
        """
        Saves the exchange rates in the database table.

        Inserts the exchange rates stored in the `exchange_rate` DataFrame into the `currency_directory` table.
        If the DataFrame is not empty, the data is converted into a list of tuples, and an SQL query is constructed
        to perform the insertion. The `execute_values` function from the `psycopg2.extras` module is used to execute
        the query with the data. The `ON CONFLICT` clause ensures that if there is a conflict on the `exchange_rate_date`,
        the existing row will be updated with the new exchange rates.
        """
        self.log.info(f"Loading currency exchange rates into the database")
        self.conn.autocommit = False
        try:
            if not self.exchange_rate.empty:
                data = [tuple(x) for x in self.exchange_rate.to_records(index=False)]
                query = f"""
                    INSERT INTO {self.schema}.{self.table_name} 
                       (exchange_rate_date, usd_rate, eur_rate, kzt_rate)
                    VALUES %s 
                    ON CONFLICT (exchange_rate_date) DO UPDATE SET 
                    usd_rate = EXCLUDED.usd_rate,
                    eur_rate = EXCLUDED.eur_rate,
                    kzt_rate = EXCLUDED.kzt_rate;"""
                self.log.info(self.exchange_rate.head())
                execute_values(self.cur, query, data)
                self.conn.commit()
                self.log.info("Currency exchange rates successfully loaded into the 'currency_directory' table.")

        except Exception as e:
            self.conn.rollback()
            self.log.error(f"An error occurred while saving data in the 'save_exchange_rate' function: {e}")
            raise

        finally:
            self.cur.close()

def exchange_rates():
    """
    Main function for running the currency exchange rates parser
    """
    log.info('Starting currency exchange rates parser')
    try:
        # Assuming conn, log, base_exchange_rates, schemes are defined somewhere
        parser = CurrencyDirectory(conn, log, base_exchange_rates, schemes, currencies)
        parser.obtaining_currency()
        parser.adapt_numpy_null()
        parser.save_exchange_rate()
        log.info('Parser successfully completed the task')
    except Exception as e:
        log.error(f'Error occurred during parser execution: {e}')

default_args = {
    "owner": "admin_1T",
    'start_date': datetime(2023, 12, 2),
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id="upd_exchange_rates",
        schedule_interval='@daily', tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as dag_exchange_rates:
    parse_get_match_jobs = PythonOperator(
        task_id='upd_exchange_rates_task',
        python_callable=exchange_rates,
        provide_context=True
    )








