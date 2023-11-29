import time
import pandas as pd
from selenium import webdriver
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import sys
import numpy as np
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin
from psycopg2.extras import execute_values
import logging as log
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.variables_settings import variables, profs

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
            'vacancy_url', 'vacancy_name', 'towns', 'level', 'company', 'salary_from',
            'salary_to', 'exp_from', 'exp_to', 'description', 'job_type', 'job_format',
            'languages', 'skills', 'source_vac', 'date_created', 'date_of_download',
            'status', 'date_closed', 'version_vac', 'actual'
        ]
        self.df = pd.DataFrame(columns=columns)
        self.dataframe_to_closed = pd.DataFrame(columns=columns)
        self.dataframe_to_update = pd.DataFrame(columns=columns)
        self.log.info("Созданы DataFrame's для записи вакансий")

        # options = webdriver.ChromeOptions()
        # options.add_argument("--headless")
        # options.add_argument('--no-sandbox')
        # options.add_argument('--disable-dev-shm-usage')
        # self.browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
        # self.browser = webdriver.Chrome(options=options)
        self.url = url
        # self.browser.get(self.url)
        # self.browser.maximize_window()
        # self.browser.delete_all_cookies()
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

    def save_df(self):
        self.log.info(f"Осуществляем загрузку данных в БД")
        try:
            if not self.df.empty:
                data = [tuple(x) for x in self.df.to_records(index=False)]
                query = f"""
                    INSERT INTO {self.schema}.{self.table_name} 
                       (vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, exp_from, exp_to, 
                       description, job_type, job_format, languages, skills, source_vac, date_created, date_of_download, 
                       status, date_closed, version_vac, actual)
                    VALUES %s 
                    ON CONFLICT (vacancy_url, version_vac) DO UPDATE SET 
                    vacancy_name = EXCLUDED.vacancy_name, 
                    towns = EXCLUDED.towns,
                    level = EXCLUDED.level,
                    company = EXCLUDED.company,
                    salary_from = EXCLUDED.salary_from, 
                    salary_to = EXCLUDED.salary_to, 
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
                self.log.info(f"Запрос вставки данных: {query}")
                print(self.df.head())
                self.log.info(self.df.head())
                execute_values(self.cur, query, data)
                self.conn.commit()
                self.log.info("Общее количество загруженных в БД вакансий: " + str(len(self.df)) + "\n")
        except Exception as e:
            self.log.error(f"Произошла ошибка при сохранении данных в функции 'save_df': {e}")
            raise

    def generating_dataframes(self):
        """
        Method for generating dataframes for data updates
        """
        try:
            if not self.df.empty:
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")

                self.log.info('Собираем вакансии для сравнения')
                query = f"""SELECT DISTINCT vacancy_url FROM {self.schema}.{self.table_name}"""
                self.cur.execute(query)
                links_in_db = self.cur.fetchall()
                links_in_db_set = set(vacancy_url for vacancy_url, in links_in_db)
                links_in_parsed = set(self.df['vacancy_url'])
                links_to_close = links_in_db_set - links_in_parsed

                self.log.info('Создаем датафрейм dataframe_to_closed')
                if links_to_close:
                    for link in links_to_close:
                        query = f"""
                            SELECT vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, exp_from, 
                            exp_to, description, job_type, job_format, languages, skills, source_vac, date_created, 
                            date_of_download, status, date_closed, version_vac, actual
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
                                    'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
                                    'description': record[9], 'job_type': record[10], 'job_format': record[11],
                                    'languages': record[12], 'skills': record[13], 'source_vac': record[14],
                                    'date_created': record[15], 'date_of_download': datetime.now().date(),
                                    'status': 'closed', 'date_closed': datetime.now().date(),
                                    'version_vac': record[-2] + 1, 'actual': -1
                                }
                                self.dataframe_to_closed = pd.concat([self.dataframe_to_closed,
                                                                      pd.DataFrame(data_to_close, index=[0])])
                    self.log.info('Датафрейм dataframe_to_closed создан')
                else:
                    self.log.info('Список links_to_close пуст')

                self.log.info('Присваиваем статусы изменений')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    link = record[0]
                    query = f"""
                        SELECT vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, exp_from, 
                            exp_to, description, job_type, job_format, languages, skills, source_vac, date_created, 
                            date_of_download, status, date_closed, version_vac, actual
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
                                    'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
                                    'description': record[9], 'job_type': record[10], 'job_format': record[11],
                                    'languages': record[12], 'skills': record[13], 'source_vac': record[14],
                                    'date_created': old_record[15], 'date_of_download': datetime.now().date(),
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
                                        'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
                                        'description': record[9], 'job_type': record[10], 'job_format': record[11],
                                        'languages': record[12], 'skills': record[13], 'source_vac': record[14],
                                        'date_created': old_record[15], 'date_of_download': datetime.now().date(),
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
                                        'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
                                        'description': record[9], 'job_type': record[10], 'job_format': record[11],
                                        'languages': record[12], 'skills': record[13], 'source_vac': record[14],
                                        'date_created': record[15], 'date_of_download': datetime.now().date(),
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
                            'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
                            'description': record[9], 'job_type': record[10], 'job_format': record[11],
                            'languages': record[12], 'skills': record[13], 'source_vac': record[14],
                            'date_created': record[15], 'date_of_download': datetime.now().date(),
                            'status': 'new', 'date_closed': record[-3], 'version_vac': 1,
                            'actual': 1
                        }
                        self.dataframe_to_update = pd.concat(
                            [self.dataframe_to_update, pd.DataFrame(data_full_new, index=[0])]
                        )

        except Exception as e:
            self.log.error(f"Ошибка при работе метода 'generating_dataframes': {e}")
            raise

    def update_database_queries(self):
        """
        Method for performing data update in the database
        """
        self.log.info('Старт обновления данных в БД')
        try:
            if not self.dataframe_to_update.empty:
                data_tuples_to_insert = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                cols = ",".join(self.dataframe_to_update.columns)
                self.log.info(f'Обновляем таблицу {self.table_name}.')
                query = f"""INSERT INTO {self.schema}.{self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 
                            len(self.dataframe_to_update.columns))})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_insert)
                self.log.info(f"Количество строк вставлено в {self.schema}.{self.table_name}: "
                              f"{len(data_tuples_to_insert)}, обновлена таблица {self.schema}.{self.table_name} "
                              f"в БД.")

            if not self.dataframe_to_closed.empty:
                self.log.info(f'Добавляем строки удаленных вакансий в таблицу {self.table_name}.')
                data_tuples_to_closed = [tuple(x) for x in self.dataframe_to_closed.to_records(index=False)]
                cols = ",".join(self.dataframe_to_closed.columns)
                query = f"""INSERT INTO {self.schema}.{self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 
                            len(self.dataframe_to_closed.columns))})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк помечено как 'closed' в {self.schema}.{self.table_name}: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица {self.schema}.{self.table_name} в БД.")
            else:
                self.log.info(f"dataframe_to_closed пуст.")

            self.conn.commit()
            self.log.info(f"Операции успешно выполнены. Изменения сохранены в таблицах.")
        except Exception as e:
            self.conn.rollback()
            self.log.error(f"Произошла ошибка: {str(e)}")
        finally:
            self.cur.close()


