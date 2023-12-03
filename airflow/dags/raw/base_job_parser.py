import time
import pandas as pd
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import sys
import numpy as np
from datetime import datetime
from psycopg2.extras import execute_values
import logging as log
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from variables_settings import variables, profs

raw_tables = variables['raw_tables']
schemes = variables["schemes"]

class BaseJobParser:
    """
    Base class for parsing job vacancies
    """
    def __init__(self, url, profs, log, conn, table_name):
        self.log = log
        self.table_name = table_name
        columns = [
            'vacancy_url', 'vacancy_name', 'towns', 'level', 'company', 'salary_from', 'salary_to', 'currency_id',
            'сurr_salary_from', 'сurr_salary_to', 'exp_from', 'exp_to', 'description', 'job_type', 'job_format',
            'languages', 'skills', 'source_vac', 'date_created', 'date_of_download', 'status', 'date_closed',
            'version_vac', 'actual'
            ]
        self.df = pd.DataFrame(columns=columns)
        self.dataframe_to_closed = pd.DataFrame(columns=columns)
        self.dataframe_to_update = pd.DataFrame(columns=columns)
        self.log.info("DataFrames for storing vacancies are created")
        self.url = url
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
        # self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # time.sleep(2)
        # new_page_height = self.browser.execute_script('return document.body.scrollHeight')
        # if new_page_height > page_height:
        #     self.scroll_down_page(new_page_height)

    def stop(self):
        """
        Method to exit Selenium Webdriver
        """
        # self.browser.quit()

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
        # self.cur = self.conn.cursor()

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        self.df = self.df.fillna(psycopg2.extensions.AsIs('NULL'))
        self.dataframe_to_closed = self.dataframe_to_closed.fillna(psycopg2.extensions.AsIs('NULL'))
        self.dataframe_to_update = self.dataframe_to_update.fillna(psycopg2.extensions.AsIs('NULL'))

    def calculate_currency_vacancies(self):
        """
        Converts currency vacancies into rubles based on the latest exchange rates.

        Raises:
            Exception: If there is an error in calculating currency vacancies.
        """
        try:
            if not self.df.empty:
                self.log.info('Currency vacancies are calculated')
                query = f"""
                        SELECT usd_rate, eur_rate, kzt_rate
                        FROM {self.schema}.currency_directory
                        WHERE exchange_rate_date = 
                        (SELECT MAX(exchange_rate_date) FROM {self.schema}.currency_directory)
                        """
                self.cur.execute(query)
                rate = self.cur.fetchall()[0]

                self.df['salary_from'] = self.df['сurr_salary_from']
                self.df['salary_to'] = self.df['сurr_salary_to']

                self.df.loc[self.df['currency_id'] == "USD", 'salary_from'] = self.df['сurr_salary_from'] * rate[0]
                self.df.loc[self.df['currency_id'] == "USD", 'salary_to'] = self.df['сurr_salary_to'] * rate[0]

                self.df.loc[self.df['currency_id'] == "EUR", 'salary_from'] = self.df['сurr_salary_from'] * rate[1]
                self.df.loc[self.df['currency_id'] == "EUR", 'salary_to'] = self.df['сurr_salary_to'] * rate[1]

                self.df.loc[self.df['currency_id'] == "KZT", 'salary_from'] = self.df['сurr_salary_from'] * rate[2]
                self.df.loc[self.df['currency_id'] == "KZT", 'salary_to'] = self.df['сurr_salary_to'] * rate[2]

                self.df.loc[
                    ~self.df['currency_id'].isin(["RUB", "USD", "EUR", "KZT"]), ['salary_from', 'salary_to']] = None

                self.log.info('The values of currency vacancies have been successfully converted into rubles '
                              'and recorded in the Dataframe')

        except Exception as e:
            self.log.error(f'Error in calculating currency vacancies: {str(e)}')

        self.df['salary_from'] = self.df['salary_from'].round(0)
        self.df['salary_to'] = self.df['salary_to'].round(0)
        # self.df['salary_from'] = self.df['salary_from'].astype(int)
        # self.df['salary_to'] = self.df['salary_to'].astype(int)

    def save_df(self):
        """
        This method saves the DataFrame to a database table. It inserts the data into the table,
        and in case of a conflict (when a record with the same vacancy_url and version_vac already exists),
        it updates the existing record with the new values.

        The method performs the following steps:
        1. Checks if the DataFrame is empty.
        2. Converts the DataFrame into a list of tuples.
        3. Constructs the SQL query for inserting the data into the table.
        4. Executes the query using the `execute_values` method.
        5. Commits the changes to the database.
        """
        self.log.info(f"Loading data into the database")
        try:
            if not self.df.empty:


                data = [tuple(x) for x in self.df.to_records(index=False)]
                query = f"""
                    INSERT INTO {self.schema}.{self.table_name} 
                       (vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, currency_id, 
                        сurr_salary_from, сurr_salary_to, exp_from, exp_to, description, job_type, job_format, 
                        languages, skills, source_vac, date_created, date_of_download, status, date_closed, 
                        version_vac, actual)
                    VALUES %s 
                    ON CONFLICT (vacancy_url, version_vac) DO UPDATE SET 
                    vacancy_name = EXCLUDED.vacancy_name, 
                    towns = EXCLUDED.towns,
                    level = EXCLUDED.level,
                    company = EXCLUDED.company,
                    salary_from = EXCLUDED.salary_from, 
                    salary_to = EXCLUDED.salary_to, 
                    currency_id = EXCLUDED.currency_id,
                    сurr_salary_from = EXCLUDED.сurr_salary_from,
                    сurr_salary_to = EXCLUDED.сurr_salary_to,
                    exp_from = EXCLUDED.exp_from, 
                    exp_to = EXCLUDED.exp_to,
                    description = EXCLUDED.description, 
                    job_type = EXCLUDED.job_type, 
                    job_format = EXCLUDED.job_format, 
                    languages = EXCLUDED.languages,
                    skills = EXCLUDED.skills,
                    source_vac = EXCLUDED.source_vac, 
                    date_created = EXCLUDED.date_created, 
                    date_of_download = EXCLUDED.date_of_download, 
                    status = EXCLUDED.status, 
                    date_closed = EXCLUDED.date_closed, 
                    version_vac = EXCLUDED.version_vac, 
                    actual = EXCLUDED.actual;"""
                self.log.info(f"Insert query: {query}")
                print(self.df.head())
                self.log.info(self.df.head())
                execute_values(self.cur, query, data)
                self.conn.commit()
                self.log.info("Total number of loaded vacancies in the database: " + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"An error occurred while saving data in the 'save_df' function: {e}")
            raise

    def generating_dataframes(self):
        """
        This method generates dataframes for further comparison and updating of vacancy records.

        It performs the following steps:
        1. Checks the data types in the DataFrame.
        2. Retrieves distinct vacancy URLs from the specified schema and table.
        3. Compares the vacancy URLs in the database with the parsed data and identifies the URLs to close.
        4. Creates a dataframe called 'dataframe_to_closed' for the records to be closed.
        5. If there are URLs to close, retrieves the relevant records from the database and adds them to 'dataframe_to_closed'.
        6. Assigns status to each record in the parsed data based on its presence in the database.
        7. Retrieves the most recent record for each URL from the database.
        8. Compares the attributes of the most recent record with the parsed data.
        9. If the record is new, adds it to 'dataframe_to_update' with a status of 'existing'.
        10. If the record is existing and has changed, adds it to 'dataframe_to_update' with a status of 'existing'.
        11. If the record is closed but has reappeared in the parsed data, adds it to 'dataframe_to_update' with a status of 'new'.
        12. If the URL is not present in the database, adds the record to 'dataframe_to_update' with a status of 'new'.

        Raises:
            Exception: If there is an error during the execution of the method.
        """
        try:
            if not self.df.empty:
                self.log.info(f"Checking data types in DataFrame: \n {self.df.dtypes}")

                self.log.info('Collecting vacancies for comparison')
                query = f"""SELECT DISTINCT vacancy_url FROM {self.schema}.{self.table_name}"""
                self.cur.execute(query)
                links_in_db = self.cur.fetchall()
                links_in_db_set = set(vacancy_url for vacancy_url, in links_in_db)
                links_in_parsed = set(self.df['vacancy_url'])
                links_to_close = links_in_db_set - links_in_parsed

                self.log.info('Creating dataframe dataframe_to_closed')
                if links_to_close:
                    for link in links_to_close:
                        query = f"""
                            SELECT vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, 
                            currency_id, сurr_salary_from, сurr_salary_to, exp_from, exp_to, description, job_type, 
                            job_format, languages, skills, source_vac, date_created, date_of_download, status, 
                            date_closed, version_vac, actual
                            FROM {self.schema}.{self.table_name}
                            WHERE vacancy_url = '{link}'
                                AND status != 'closed'
                                AND actual != '-1'
                                AND version_vac = (
                                    SELECT max(version_vac) FROM {self.schema}.{self.table_name}
                                    WHERE vacancy_url = '{link}'
                                )
                            ORDER BY date_of_download DESC, version_vac DESC
                            LIMIT 1
                            """
                        self.cur.execute(query)
                        records_to_close = self.cur.fetchall()

                        if records_to_close:
                            for record in records_to_close:
                                data_to_close = {
                                    'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'level': record[3], 'company': record[4], 'salary_from': record[5],
                                    'salary_to': record[6], 'currency_id': record[7], 'сurr_salary_from': record[8],
                                    'сurr_salary_to': record[9], 'exp_from': record[10], 'exp_to': record[11],
                                    'description': record[12], 'job_type': record[13], 'job_format': record[14],
                                    'languages': record[15], 'skills': record[16], 'source_vac': record[17],
                                    'date_created': record[18], 'date_of_download': datetime.now().date(),
                                    'status': 'closed', 'date_closed': datetime.now().date(),
                                    'version_vac': record[-2] + 1, 'actual': -1
                                }
                                self.dataframe_to_closed = pd.concat([self.dataframe_to_closed,
                                                                      pd.DataFrame(data_to_close, index=[0])])
                        self.log.info('Dataframe dataframe_to_closed created')
                else:
                    self.log.info('The links_to_close list is empty')

                self.log.info('Assigning change statuses')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    link = record[0]
                    query = f"""
                        SELECT vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, currency_id, 
                            сurr_salary_from, сurr_salary_to, exp_from, exp_to, description, job_type, job_format, 
                            languages, skills, source_vac, date_created, date_of_download, status, date_closed, 
                            version_vac, actual
                        FROM {self.schema}.{self.table_name}
                        WHERE vacancy_url = '{link}'
                        ORDER BY date_of_download DESC, version_vac DESC
                        LIMIT 1
                        """
                    self.cur.execute(query)
                    records_in_db = self.cur.fetchall()

                    if records_in_db:
                        for old_record in records_in_db:
                            old_status = old_record[-4]
                            next_version = old_record[-2] + 1

                            if old_status == 'new':
                                data_new_vac = {
                                    'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'level': record[3], 'company': record[4], 'salary_from': record[5],
                                    'salary_to': record[6], 'currency_id': record[7], 'сurr_salary_from': record[8],
                                    'сurr_salary_to': record[9], 'exp_from': record[10], 'exp_to': record[11],
                                    'description': record[12], 'job_type': record[13], 'job_format': record[14],
                                    'languages': record[15], 'skills': record[16], 'source_vac': record[17],
                                    'date_created': old_record[18], 'date_of_download': datetime.now().date(),
                                    'status': 'existing', 'date_closed': old_record[-3], 'version_vac': next_version,
                                    'actual': 1
                                }
                                self.dataframe_to_update = pd.concat(
                                    [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                )

                            elif old_status == 'existing':
                                if pd.Series(old_record[:13]).equals(pd.Series(record[:13])):
                                    pass

                                else:
                                    data_new_vac = {
                                        'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'level': record[3], 'company': record[4], 'salary_from': record[5],
                                        'salary_to': record[6], 'currency_id': record[7], 'сurr_salary_from': record[8],
                                        'сurr_salary_to': record[9], 'exp_from': record[10], 'exp_to': record[11],
                                        'description': record[12], 'job_type': record[13], 'job_format': record[14],
                                        'languages': record[15], 'skills': record[16], 'source_vac': record[17],
                                        'date_created': old_record[18], 'date_of_download': datetime.now().date(),
                                        'status': 'existing', 'date_closed': old_record[-3],
                                        'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                    )
                            elif old_status == 'closed':
                                if link in links_in_parsed:
                                    data_clos_new = {
                                        'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'level': record[3], 'company': record[4], 'salary_from': record[5],
                                        'salary_to': record[6], 'currency_id': record[7], 'сurr_salary_from': record[8],
                                        'сurr_salary_to': record[9], 'exp_from': record[10], 'exp_to': record[11],
                                        'description': record[12], 'job_type': record[13], 'job_format': record[14],
                                        'languages': record[15], 'skills': record[16], 'source_vac': record[17],
                                        'date_created': record[18], 'date_of_download': datetime.now().date(),
                                        'status': 'new', 'date_closed': record[-3], 'version_vac': next_version,
                                        'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_clos_new, index=[0])]
                                    )
                    else:
                        data_full_new = {
                            'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
                            'level': record[3], 'company': record[4], 'salary_from': record[5],
                            'salary_to': record[6], 'currency_id': record[7], 'сurr_salary_from': record[8],
                            'сurr_salary_to': record[9], 'exp_from': record[10], 'exp_to': record[11],
                            'description': record[12], 'job_type': record[13], 'job_format': record[14],
                            'languages': record[15], 'skills': record[16], 'source_vac': record[17],
                            'date_created': record[18], 'date_of_download': datetime.now().date(),
                            'status': 'new', 'date_closed': record[-3], 'version_vac': 1,
                            'actual': 1
                        }
                        self.dataframe_to_update = pd.concat(
                            [self.dataframe_to_update, pd.DataFrame(data_full_new, index=[0])]
                        )

        except Exception as e:
            self.log.error(f"Error in 'generating_dataframes' method: {e}")
            raise

    def update_database_queries(self):
        """
        Method for performing data update in the database

        This method updates the data in the database by inserting new rows or marking existing rows as 'closed'.
        It takes the data from the `dataframe_to_update` and `dataframe_to_closed` DataFrames and inserts them into
        the specified table in the database.
        Raises:
            Exception: If an error occurs during the data update process.
        """
        self.log.info('Start updating data in the database.')
        try:
            if not self.dataframe_to_update.empty:
                data_tuples_to_insert = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                cols = ",".join(self.dataframe_to_update.columns)
                self.log.info(f'Updating table {self.table_name}.')
                query = f"""INSERT INTO {self.schema}.{self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 
                            len(self.dataframe_to_update.columns))})"""
                self.log.info(f"Insert query: {query}")
                self.cur.executemany(query, data_tuples_to_insert)
                self.log.info(f"Number of rows inserted into {self.schema}.{self.table_name}: "
                              f"{len(data_tuples_to_insert)}, {self.schema}.{self.table_name} table updated in the database.")

            if not self.dataframe_to_closed.empty:
                self.log.info(f'Adding closed vacancies rows to the {self.table_name} table.')
                data_tuples_to_closed = [tuple(x) for x in self.dataframe_to_closed.to_records(index=False)]
                cols = ",".join(self.dataframe_to_closed.columns)
                query = f"""INSERT INTO {self.schema}.{self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 
                            len(self.dataframe_to_closed.columns))})"""
                self.log.info(f"Insert query: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Number of rows marked as 'closed' in {self.schema}.{self.table_name}: "
                              f"{len(data_tuples_to_closed)}, {self.schema}.{self.table_name} table updated in the database.")
            else:
                self.log.info(f"dataframe_to_closed is empty.")

            self.conn.commit()
            self.log.info(f"Operations completed successfully. Changes saved in the tables.")
        except Exception as e:
            self.conn.rollback()
            self.log.error(f"An error occurred: {str(e)}")
        finally:
            self.cur.close()
