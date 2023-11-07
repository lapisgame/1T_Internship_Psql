import dateparser
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv, json
import psycopg2
from airflow import settings
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from airflow import settings
from sqlalchemy import create_engine
from airflow import DAG
from selenium.common.exceptions import NoSuchElementException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import logging
from logging import handlers
from airflow import models
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import time
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options as ChromeOptions
import pandas as pd
import numpy as np
import os
# Connections settings
# Загружаем данные подключений из JSON файла
with open('/opt/airflow/dags/config_connections.json', 'r') as conn_file:
    connections_config = json.load(conn_file)

# Получаем данные конфигурации подключения и создаем конфиг для клиента
conn_config = connections_config['psql_connect']

config = {
    'database': conn_config['database'],
    'user': conn_config['user'],
    'password': conn_config['password'],
    'host': conn_config['host'],
    'port': conn_config['port'],
}

conn = psycopg2.connect(**config)
conn.autocommit = False

# Variables settings
# Загружаем переменные из JSON файла
with open('/opt/airflow/dags/config_variables.json', 'r') as config_file:
    my_variables = json.load(config_file)

# Проверяем, существует ли переменная с данным ключом
if not Variable.get("shares_variable", default_var=None):
    # Если переменная не существует, устанавливаем ее
    Variable.set("shares_variable", my_variables, serialize_json=True)

dag_variables = Variable.get("shares_variable", deserialize_json=True)

url_sber = dag_variables.get('base_sber')
url_yand = dag_variables.get('base_yand')
url_vk = dag_variables.get('base_vk')
url_tin = dag_variables.get('base_tin')

raw_tables = ['raw_vk', 'raw_sber', 'raw_tinkoff', 'raw_yandex']

options = ChromeOptions()

profs = dag_variables.get('professions')

logging_level = os.environ.get('LOGGING_LEVEL', 'DEBUG').upper()
logging.basicConfig(level=logging_level)
log = logging.getLogger(__name__)
log_handler = handlers.RotatingFileHandler('/opt/airflow/logs/airflow.log',
                                           maxBytes=5000,
                                           backupCount=5)

log_handler.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)
log.addHandler(log_handler)

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    # 'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
}


# Создаем DAG для автоматического запуска каждые 6 часов
updated_raw_dag=DAG(dag_id='updated_raw_dag',
                tags=['admin_1T'],
                start_date=datetime(2023, 11, 5),
                schedule_interval='@daily',
                default_args=default_args
                )

class BaseJobParser:
    def __init__(self, url, profs, log, conn):
        self.browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
        self.url = url
        self.browser.get(self.url)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.profs = profs
        self.log = log
        self.conn = conn

    def scroll_down_page(self, page_height=0):
        """
        Метод прокрутки страницы
        """
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_page_height = self.browser.execute_script('return document.body.scrollHeight')
        if new_page_height > page_height:
            self.scroll_down_page(new_page_height)

    def stop(self):
        """
        Метод для выхода из Selenium Webdriver
        """
        self.browser.quit()

    def find_vacancies(self):
        """
        Метод для парсинга вакансий, должен быть переопределен в наследниках
        """
        raise NotImplementedError("Вы должны определить метод find_vacancies")

    def save_df(self):
        """
        Метод для сохранения данных из pandas DataFrame
        """
        raise NotImplementedError("Вы должны определить метод save_df")

    def update_database_queries(self):
        """
        Метод для выполнения запросов к базе данных.
        """
        raise NotImplementedError("Вы должны определить метод save_df")


class VKJobParser(BaseJobParser):
    """
    Парсер вакансий с сайта VK, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с VK
        """
        self.df = pd.DataFrame(
            columns=['vacancy_id', 'vacancy_name', 'towns', 'company', 'description', 'source_vac', 'date_created',
                     'date_of_download', 'status', 'date_closed', 'version_vac', 'actual'])
        self.log.info("Создан DataFrame для записи вакансий")
        self.browser.implicitly_wait(3)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs:
            input_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/div/div/div/input')
            input_button.send_keys(prof['fullName'])
            click_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/button')
            click_button.click()
            time.sleep(5)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div[1]/div[2]/div/div')
                vacs = vacs_bar.find_elements(By.CLASS_NAME, 'result-item')
                vacs = [div for div in vacs if 'result-item' in str(div.get_attribute('class'))]
                self.log.info(f"Парсим вакансии по запросу: {prof['fullName']}")
                self.log.info(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['vacancy_id'] = str(vac.get_attribute('href'))
                    vac_info['vacancy_name'] = str(vac.find_element(By.CLASS_NAME, 'title-block').text)
                    vac_info['towns'] = str(vac.find_element(By.CLASS_NAME, 'result-item-place').text)
                    vac_info['company'] = str(vac.find_element(By.CLASS_NAME, 'result-item-unit').text)
                    self.df.loc[len(self.df)] = vac_info

                self.df['date_created'] = datetime.now().date()
                self.df['date_of_download'] = datetime.now().date()
                self.df['source_vac'] = url_vk
                self.df['status'] = 'existing'
                self.df['actual'] = 1

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_button.clear()

        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для VKJobParser.
        """
        if not self.df.empty:
            self.log.info('Старт парсинга описаний вакансий')
            for descr in self.df.index:
                try:
                    vacancy_id = self.df.loc[descr, 'vacancy_id']
                    self.browser.get(vacancy_id)
                    self.browser.delete_all_cookies()
                    time.sleep(3)
                    desc = self.browser.find_element(By.CLASS_NAME, 'section').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'vacancy_id']}")
                    pass

        else:
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных vk
        """
        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        try:
            if not self.df.empty:
                self.cur = self.conn.cursor()
                self.table_name = raw_tables[0]
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")

                self.log.info('Собираем вакансии для сравнения')
                query = f"""SELECT vacancy_id FROM {self.table_name}
                            WHERE version_vac = (SELECT max(version_vac) FROM {self.table_name})
                            ORDER BY date_of_download DESC, version_vac DESC LIMIT 1"""
                self.cur.execute(query)
                links_in_db = self.cur.fetchall()
                links_in_db_set = set(vacancy_id for vacancy_id, in links_in_db)
                links_in_parsed = set(self.df['vacancy_id'])
                links_to_close = links_in_db_set - links_in_parsed
                self.dataframe_to_closed = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'towns', 'company', 'description', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'date_closed', 'version_vac', 'actual'
                ])
                self.dataframe_to_update = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'towns', 'company', 'description', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'version_vac', 'actual'
                ])

                self.log.info('Создаем датафрейм dataframe_to_closed')
                if links_to_close:
                    for link in links_to_close:
                        query = f"""
                            SELECT vacancy_id, vacancy_name, towns, company, description, source_vac,
                                   date_created, date_of_download, status, date_closed, version_vac, actual
                            FROM {self.table_name}
                            WHERE vacancy_id = '{link}'
                                AND status != 'closed'
                                AND actual != '-1'
                                AND version_vac = (
                                    SELECT max(version_vac) FROM {self.table_name}
                                    WHERE vacancy_id = '{link}'
                                )
                            ORDER BY date_of_download DESC, version_vac DESC
                            LIMIT 1
                            """
                        self.cur.execute(query)
                        records_to_close = self.cur.fetchall()

                        if records_to_close:
                            for record in records_to_close:
                                data = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'company': record[3], 'description': record[4], 'source_vac': record[5],
                                    'date_created': record[6], 'date_of_download': datetime.now().date(),
                                    'status': 'closed', 'date_closed': datetime.now().date(),
                                    'version_vac': record[-2] + 1, 'sign': -1
                                }
                                self.dataframe_to_closed = pd.concat([self.dataframe_to_closed, pd.DataFrame(data, index=[0])])
                    self.log.info('Датафрейм dataframe_to_closed создан')
                else:
                    self.log.info('Список links_to_close пуст')

                self.log.info('Присваиваем статусы изменений')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    link = record[0]
                    query = f"""
                        SELECT vacancy_id, vacancy_name, towns, company, description, source_vac,
                               date_created, date_of_download, status, version_vac, actual
                        FROM {self.table_name}
                        WHERE vacancy_id = '{link}'
                        ORDER BY date_of_download DESC, version_vac DESC
                        LIMIT 1
                        """
                    self.cur.execute(query)
                    records_in_db = self.cur.fetchall()

                    if records_in_db:
                        for old_record in records_in_db:
                            old_status = old_record[-3]
                            next_version = old_record[-2] + 1

                            if old_status == 'new':
                                data_new_vac = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'company': record[3], 'description': record[4],
                                    'source_vac': record[5], 'date_created': old_record[6],
                                    'date_of_download': datetime.now().date(), 'status': 'existing',
                                    'version_vac': next_version, 'actual': 1
                                }
                                self.dataframe_to_update = pd.concat(
                                    [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                )
                            elif old_status == 'existing':
                                if old_record[1] == record[1] and old_record[2] == record[2] and \
                                    old_record[3] == record[3] and old_record[4] == record[4] and \
                                    old_record[5] == record[5] and old_record[6] == record[6]:
                                    pass

                                else:
                                    data_new_vac = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'company': record[3], 'description': record[4],
                                        'source_vac': record[5], 'date_created': old_record[6],
                                        'date_of_download': datetime.now().date(), 'status': 'existing',
                                        'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                    )
                            elif old_status == 'closed':
                                if link in links_in_parsed:
                                    data_clos_new = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'company': record[3], 'description': record[4],
                                        'source_vac': record[5], 'date_created': record[6],
                                        'date_of_download': datetime.now().date(), 'status': 'new',
                                        'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_clos_new, index=[0])]
                                    )
                                else:
                                    data_full_new = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'company': record[3], 'description': record[4],
                                        'source_vac': record[5], 'date_created': record[6],
                                        'date_of_download': datetime.now().date(), 'status': 'new', 'version_vac': 1,
                                        'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_full_new, index=[0])]
                                    )

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise

    def update_database_queries(self):
        """
        Метод для выполнения запросов к базе данных.
        """
        self.cur = self.conn.cursor()

        try:
            if not self.dataframe_to_closed.empty:
                self.log.info(f'Добавляем строки удаленных вакансий в таблицу {self.table_name}.')
                data_tuples_to_closed = [tuple(x) for x in self.dataframe_to_closed.to_records(index=False)]
                cols = ",".join(self.dataframe_to_closed.columns)
                query = f"""INSERT INTO {self.table_name} ({cols}) VALUES (%s)"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк удалено из core_fact_table: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица {self.table_name} в БД "
                              f"{config['database']}.")

                self.log.info(f'Вставляем строки удаленных вакансий в таблицу del_vacancy_core.')
                query = f"""INSERT INTO del_vacancy_core ({cols}) VALUES %s"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк вставлено в del_vacancy_core: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица del_vacancy_core в БД "
                              f"{config['database']}.")

                self.log.info(f'Удаляем из core_fact_table закрытые вакансии.')
                data_to_delete_tuples = [tuple(x) for x in
                                         self.dataframe_to_closed[['link']].to_records(index=False)]
                for to_delete in data_to_delete_tuples:
                    query = f"""DELETE FROM core_fact_table WHERE link = '{to_delete[0]}'"""
                    self.log.info(f"Запрос вставки данных: {query}")
                    self.cur.executemany(query, data_to_delete_tuples)
                    self.log.info(f"Количество строк удалено из core_fact_table: "
                                  f"{len(data_to_delete_tuples)}, обновлена таблица core_fact_table в БД "
                                  f"{config['database']}.")

            else:
                self.log.info(f"dataframe_to_closed пуст.")

            if not self.dataframe_to_update.empty:
                data_tuples_to_insert = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                cols = ",".join(self.dataframe_to_update.columns)
                self.log.info(f'Обновляем таблицу {self.table_name}.')
                query = f"""INSERT INTO {self.table_name} ({cols}) VALUES (%s)"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_insert)
                self.log.info(f"Количество строк вставлено в {self.table_name}: "
                              f"{len(data_tuples_to_insert)}, обновлена таблица {self.table_name} "
                              f"в БД {config['database']}.")

                self.log.info(f'Обновляем таблицу core_fact_table.')
                core_fact_data_tuples = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                query = f"""INSERT INTO core_fact_table ({cols}) VALUES %s"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, core_fact_data_tuples)
                self.log.info(f"Количество строк вставлено в core_fact_table: "
                              f"{len(core_fact_data_tuples)}, обновлена таблица core_fact_table "
                              f"в БД {config['database']}.")
            else:
                self.log.info(f"dataframe_to_update пуст.")

            self.conn.commit()
            self.log.info(f"Операции успешно выполнены. Изменения сохранены в таблицах.")
        except Exception as e:
            self.conn.rollback()
            self.log.error(f"Произошла ошибка: {str(e)}")
        finally:
            self.cur.close()

class SberJobParser(BaseJobParser):
    """
    Парсер для вакансий с сайта Sberbank, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с Sberbank
        """
        self.cur = self.conn.cursor()
        self.df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'towns', 'company', 'description', 'source_vac',
                                        'date_created', 'date_of_download', 'status', 'version_vac', 'actual'])
        self.log.info("Создан DataFrame для записи вакансий")
        self.browser.implicitly_wait(1)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs:
            self.browser.implicitly_wait(10)
            input_str = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]/div/div/div[2]'
                                                            '/div/div/div/div/input')

            input_str.send_keys(f"{prof['fullName']}")
            click_button = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]'
                                                               '/div/div/div[2]/div/div/div/div/button')
            click_button.click()
            time.sleep(5)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                # Подсчет количества предложений
                self.browser.implicitly_wait(10)
                vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div/div[2]/div[3]'
                                                               '/div/div/div[3]/div/div[3]/div[2]')
                vacs = vacs_bar.find_elements(By.TAG_NAME, 'div')

                vacs = [div for div in vacs if 'styled__Card-sc-192d1yv-1' in str(div.get_attribute('class'))]
                self.log.info(f"Парсим вакансии по запросу: {prof['fullName']}")
                self.log.info(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['vacancy_id'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    data = vac.find_elements(By.CLASS_NAME, 'Text-sc-36c35j-0')
                    vac_info['vacancy_name'] = data[0].text
                    vac_info['towns'] = data[2].text
                    vac_info['company'] = data[3].text
                    vac_info['date_created'] = data[4].text
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_str.clear()

        # Удаление дубликатов в DataFrame
        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: "
                      + str(len(self.df)) + "\n")
        self.df['source_vac'] = url_sber
        self.df['date_created'] = self.df['date_created'].apply(lambda x: dateparser.parse(x, languages=['ru']))
        self.df['date_created'] = pd.to_datetime(self.df['date_created']).dt.to_pydatetime()
        self.df['date_of_download'] = datetime.now().date()
        self.df['status'] = 'existing'
        self.df['description'] = None
        self.df['actual'] = 1

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для SberJobParser.
        """
        if not self.df.empty:
            self.log.info('Старт парсинга описаний вакансий')
            for descr in self.df.index:
                try:
                    vacancy_id = self.df.loc[descr, 'vacancy_id']
                    self.browser.get(vacancy_id)
                    self.browser.delete_all_cookies()
                    self.browser.implicitly_wait(10)
                    desc = self.browser.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[3]'
                                                               '/div/div/div[3]/div[3]').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'vacancy_id']}")
        else:
            self.log.info(f"Нет описаний вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Sber
        """
        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        self.df['date_created'] = self.df['date_created'].dt.date

        try:
            if not self.df.empty:
                self.cur = self.conn.cursor()
                self.df['date_created'] = datetime.now().date()
                self.df['date_of_download'] = datetime.now().date()
                self.df['source_vac'] = url_sber
                self.df['status'] = 'existing'
                self.df['actual'] = 1
                self.table_name = raw_tables[1]
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")

                self.log.info('Собираем вакансии для сравнения')
                query = f"""SELECT vacancy_id FROM {self.table_name}
                            WHERE version_vac = (SELECT max(version_vac) FROM {self.table_name})
                            ORDER BY date_of_download DESC, version_vac DESC LIMIT 1"""
                self.cur.execute(query)
                links_in_db = self.cur.fetchall()
                links_in_db_set = set(vacancy_id for vacancy_id, in links_in_db)
                links_in_parsed = set(self.df['vacancy_id'])
                links_to_close = links_in_db_set - links_in_parsed
                self.dataframe_to_closed = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'towns', 'company', 'description', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'date_closed', 'version_vac', 'actual'
                ])
                self.dataframe_to_update = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'towns', 'company', 'description', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'version_vac', 'actual'
                ])

                self.log.info('Создаем датафрейм dataframe_to_closed')
                if links_to_close:
                    for link in links_to_close:
                        query = f"""
                            SELECT vacancy_id, vacancy_name, towns, company, description, source_vac,
                                   date_created, date_of_download, status, date_closed, version_vac, actual
                            FROM {self.table_name}
                            WHERE vacancy_id = '{link}'
                                AND status != 'closed'
                                AND actual != '-1'
                                AND version_vac = (
                                    SELECT max(version_vac) FROM {self.table_name}
                                    WHERE vacancy_id = '{link}'
                                )
                            ORDER BY date_of_download DESC, version_vac DESC
                            LIMIT 1
                            """
                        self.cur.execute(query)
                        records_to_close = self.cur.fetchall()
                        if records_to_close:
                            for record in records_to_close:
                                data = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'company': record[3], 'description': record[4], 'source_vac': record[5],
                                    'date_created': record[6], 'date_of_download': datetime.now().date(),
                                    'status': 'closed', 'date_closed': datetime.now().date(),
                                    'version_vac': record[-2] + 1, 'sign': -1
                                }
                                self.dataframe_to_closed = pd.concat([self.dataframe_to_closed, pd.DataFrame(data, index=[0])])
                    self.log.info('Датафрейм dataframe_to_closed создан')
                else:
                    self.log.info('Список links_to_close пуст')

                self.log.info('Присваиваем статусы изменений')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    link = record[0]
                    query = f"""
                        SELECT vacancy_id, vacancy_name, towns, company, description, source_vac,
                               date_created, date_of_download, status, version_vac, actual
                        FROM {self.table_name}
                        WHERE vacancy_id = '{link}'
                        ORDER BY date_of_download DESC, version_vac DESC
                        LIMIT 1
                        """
                    self.cur.execute(query)
                    records_in_db = self.cur.fetchall()
                    if records_in_db:
                        for old_record in records_in_db:
                            old_status = old_record[-3]
                            next_version = old_record[-2] + 1

                            if old_status == 'new':
                                data_new_vac = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'company': record[3], 'description': record[4],
                                    'source_vac': record[5], 'date_created': old_record[6],
                                    'date_of_download': datetime.now().date(), 'status': 'existing',
                                    'version_vac': next_version, 'actual': 1
                                }
                                self.dataframe_to_update = pd.concat(
                                    [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                )
                            elif old_status == 'existing':
                                if old_record[1] == record[1] and old_record[2] == record[2] and \
                                        old_record[3] == record[3] and old_record[4] == record[4] and \
                                        old_record[5] == record[5] and old_record[6] == record[6]:
                                    pass
                                else:
                                    data_new_vac = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'company': record[3], 'description': record[4],
                                        'source_vac': record[5], 'date_created': old_record[6],
                                        'date_of_download': datetime.now().date(), 'status': 'existing',
                                        'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                    )
                            elif old_status == 'closed':
                                if link in links_in_parsed:
                                    data_clos_new = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'company': record[3], 'description': record[4],
                                        'source_vac': record[5], 'date_created': record[6],
                                        'date_of_download': datetime.now().date(), 'status': 'new',
                                        'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_clos_new, index=[0])]
                                    )
                                else:
                                    data_full_new = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'company': record[3], 'description': record[4],
                                        'source_vac': record[5], 'date_created': record[6],
                                        'date_of_download': datetime.now().date(), 'status': 'new', 'version_vac': 1,
                                        'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_full_new, index=[0])]
                                    )

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise

    def update_database_queries(self):
        """
        Метод для выполнения запросов к базе данных.
        """
        self.cur = self.conn.cursor()

        try:
            if not self.dataframe_to_closed.empty:
                self.log.info(f'Добавляем строки удаленных вакансий в таблицу {self.table_name}.')
                data_tuples_to_closed = [tuple(x) for x in self.dataframe_to_closed.to_records(index=False)]
                cols = ",".join(self.dataframe_to_closed.columns)
                query = f"""INSERT INTO {self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 12)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк удалено из core_fact_table: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица {self.table_name} в БД "
                              f"{config['database']}.")

                self.log.info(f'Вставляем строки удаленных вакансий в таблицу del_vacancy_core.')
                query = f"""INSERT INTO del_vacancy_core ({cols}) VALUES ({", ".join(["%s"] * 12)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк вставлено в del_vacancy_core: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица del_vacancy_core в БД "
                              f"{config['database']}.")

                self.log.info(f'Удаляем из core_fact_table закрытые вакансии.')
                data_to_delete_tuples = [tuple(x) for x in
                                         self.dataframe_to_closed[['link']].to_records(index=False)]
                for to_delete in data_to_delete_tuples:
                    query = f"""DELETE FROM core_fact_table WHERE link = '{to_delete[0]}'"""
                    self.log.info(f"Запрос вставки данных: {query}")
                    self.cur.executemany(query, data_to_delete_tuples)
                    self.log.info(f"Количество строк удалено из core_fact_table: "
                                  f"{len(data_to_delete_tuples)}, обновлена таблица core_fact_table в БД "
                                  f"{config['database']}.")

            else:
                self.log.info(f"dataframe_to_closed пуст.")

            if not self.dataframe_to_update.empty:
                data_tuples_to_insert = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                cols = ",".join(self.dataframe_to_update.columns)
                self.log.info(f'Обновляем таблицу {self.table_name}.')
                query = f"""INSERT INTO {self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 11)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_insert)
                self.log.info(f"Количество строк вставлено в {self.table_name}: "
                              f"{len(data_tuples_to_insert)}, обновлена таблица {self.table_name} "
                              f"в БД {config['database']}.")

                self.log.info(f'Обновляем таблицу core_fact_table.')
                core_fact_data_tuples = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                query = f"""INSERT INTO core_fact_table ({cols}) VALUES ({", ".join(["%s"] * 11)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, core_fact_data_tuples)
                self.log.info(f"Количество строк вставлено в core_fact_table: "
                              f"{len(core_fact_data_tuples)}, обновлена таблица core_fact_table "
                              f"в БД {config['database']}.")
            else:
                self.log.info(f"dataframe_to_update пуст.")


            self.conn.commit()
            self.log.info(f"Операции успешно выполнены. Изменения сохранены в таблицах.")
        except Exception as e:
            self.conn.rollback()
            self.log.error(f"Произошла ошибка: {str(e)}")
        finally:
            self.cur.close()

class TinkoffJobParser(BaseJobParser):
    """
    Парсер вакансий с сайта Tinkoff, наследованный от BaseJobParser
    """
    def open_all_pages(self):
        self.log.info('Работает функция open_all_pages')
        self.browser.implicitly_wait(10)
        elements = self.browser.find_elements(By.CLASS_NAME, 'fuBQPo')
        for element in elements:
            element.click()
        self.log.info('Работает успешно завершена')

    def all_vacs_parser(self):
        """
        Метод для нахождения вакансий с Tinkoff
        """
        self.cur = self.conn.cursor()

        self.df = pd.DataFrame(
            columns=['vacancy_id', 'vacancy_name', 'towns', 'level', 'company', 'description', 'source_vac',
                     'date_created', 'date_of_download', 'status', 'date_closed', 'version_vac', 'actual'])
        self.log.info("Создан DataFrame для записи вакансий")

        try:
            # vac_index = 0
            # while True:
            #     try:
            #         vac = self.browser.find_elements(By.CLASS_NAME, 'eM3bvP')[vac_index]
            #     except IndexError:
            #         break  # Закончили обработку всех элементов

            #     self.log.info(f"Обработка вакансии номер {vac_index + 1}")

            self.browser.implicitly_wait(3)
            vacs = self.browser.find_elements(By.CLASS_NAME, 'eM3bvP')
            for vac in vacs:
                try:
                    vac_info = {}
                    vac_info['vacancy_id'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    data = vac.find_elements(By.CLASS_NAME, 'gM3bvP')
                    vac_info['vacancy_name'] = data[0].text
                    vac_info['level'] = data[1].text
                    vac_info['towns'] = data[2].text
                    self.df.loc[len(self.df)] = vac_info
                except Exception as e:
                    log.error(f"Произошла ошибка: {e}, ссылка на вакансию: {vac_info['vacancy_id']}")

            self.df = self.df.drop_duplicates()
            self.log.info("Общее количество найденных вакансий после удаления дубликатов: "
                          + str(len(self.df)) + "\n")
            self.df['company'] = 'Тинькофф'
            self.df['description'] = None

            self.log.info(
                f"Парсер завершил работу. Обработано {len(self.df)} вакансий. Оставлены только уникальные записи. "
                f"Записи обновлены данными о компании, дате создания и загрузки.")

        except Exception as e:
            self.log.error(f"Произошла ошибка: {e}")

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для TinkoffJobParser.
        """
        if not self.df.empty:
            for descr in self.df.index:
                try:
                    vacancy_id = self.df.loc[descr, 'vacancy_id']
                    self.browser.get(vacancy_id)
                    self.browser.delete_all_cookies()
                    self.browser.implicitly_wait(3)
                    desc = str(self.browser.find_element(By.CLASS_NAME, 'dyzaXu').text)
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = desc

                    self.log.info(f"Описание успешно добавлено для вакансии {descr + 1}")

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'vacancy_id']}")
                    pass
        else:
            self.log.info(f"Нет описания вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Tinkoff
        """
        self.cur = self.conn.cursor()

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        self.df['description'] = self.df['description'].astype(str)

        try:
            if not self.df.empty:
                self.cur = self.conn.cursor()
                self.df['date_created'] = datetime.now().date()
                self.df['date_of_download'] = datetime.now().date()
                self.df['source_vac'] = url_tin
                self.df['status'] = 'existing'
                self.df['actual'] = 1
                self.table_name = raw_tables[2]
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")

                self.log.info('Собираем вакансии для сравнения')
                query = f"""SELECT vacancy_id FROM {self.table_name}
                                 WHERE version_vac = (SELECT max(version_vac) FROM {self.table_name})
                                 ORDER BY date_of_download DESC, version_vac DESC LIMIT 1"""
                self.cur.execute(query)
                links_in_db = self.cur.fetchall()
                links_in_db_set = set(vacancy_id for vacancy_id, in links_in_db)
                links_in_parsed = set(self.df['vacancy_id'])
                links_to_close = links_in_db_set - links_in_parsed

                self.dataframe_to_closed = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'towns', 'level', 'company', 'description', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'date_closed', 'version_vac', 'actual'
                ])

                self.dataframe_to_update = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'towns', 'level', 'company', 'description', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'version_vac', 'actual'
                ])

                self.log.info('Создаем датафрейм dataframe_to_closed')
                if links_to_close:
                    for link in links_to_close:
                        query =  f"""
                                 SELECT vacancy_id, vacancy_name, towns, level, company, description, source_vac,
                                        date_created, date_of_download, status, date_closed, version_vac, actual
                                 FROM {self.table_name}
                                 WHERE vacancy_id = '{link}'
                                     AND status != 'closed'
                                     AND actual != '-1'
                                     AND version_vac = (
                                         SELECT max(version_vac) FROM {self.table_name}
                                         WHERE vacancy_id = '{link}'
                                     )
                                 ORDER BY date_of_download DESC, version_vac DESC
                                 LIMIT 1
                                 """
                        self.cur.execute(query)
                        records_to_close = self.cur.fetchall()
                        if records_to_close:
                            for record in records_to_close:
                                data = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'level': record[3], 'company': record[4], 'description': record[5],
                                    'source_vac': record[6], 'date_created': record[7],
                                    'date_of_download': datetime.now().date(), 'status': 'closed',
                                    'date_closed': datetime.now().date(), 'version_vac': record[-2] + 1, 'sign': -1
                                }
                                self.dataframe_to_closed = pd.concat([self.dataframe_to_closed,
                                                                      pd.DataFrame(data, index=[0])])
                    self.log.info('Датафрейм dataframe_to_closed создан')
                else:
                    self.log.info('Список links_to_close пуст')

                self.log.info('Присваиваем статусы изменений')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    link = record[0]
                    query = f"""
                        SELECT vacancy_id, vacancy_name, towns, level, company, description, source_vac,
                               date_created, date_of_download, status, version_vac, actual
                        FROM {self.table_name}
                        WHERE vacancy_id = '{link}'
                        ORDER BY date_of_download DESC, version_vac DESC
                        LIMIT 1
                        """
                    self.cur.execute(query)
                    records_in_db = self.cur.fetchall()
                    if records_in_db:
                        for old_record in records_in_db:
                            old_status = old_record[-3]
                            next_version = old_record[-2] + 1

                            if old_status == 'new':
                                data_new_vac = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'level': record[3], 'company': record[4], 'description': record[5],
                                    'source_vac': record[6], 'date_created': old_record[7],
                                    'date_of_download': datetime.now().date(), 'status': 'existing',
                                    'version_vac': next_version, 'actual': 1
                                }
                                self.dataframe_to_update = pd.concat(
                                    [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                )
                            elif old_status == 'existing':
                                if old_record[1] == record[1] and old_record[2] == record[2] and \
                                    old_record[3] == record[3] and old_record[4] == record[4] and \
                                    old_record[5] == record[5] and old_record[6] == record[6]:
                                    pass

                                else:
                                    data_new_vac = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'level': record[3], 'company': record[4], 'description': record[5],
                                        'source_vac': record[6], 'date_created': old_record[7],
                                        'date_of_download': datetime.now().date(), 'status': 'existing',
                                        'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                    )
                            elif old_status == 'closed':
                                if link in links_in_parsed:
                                    data_clos_new = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'level': record[3], 'company': record[4], 'description': record[5],
                                        'source_vac': record[6], 'date_created': record[7],
                                        'date_of_download': datetime.now().date(), 'status': 'new',
                                        'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_clos_new, index=[0])]
                                    )
                                else:
                                    data_full_new = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'level': record[3], 'company': record[4], 'description': record[5],
                                        'source_vac': record[6], 'date_created': record[7],
                                        'date_of_download': datetime.now().date(), 'status': 'new', 'version_vac': 1,
                                        'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_full_new, index=[0])]
                                    )
                    else:
                        self.log.info(f"records_in_db пуст.")

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise

    def update_database_queries(self):
        """
        Метод для выполнения запросов к базе данных.
        """
        self.cur = self.conn.cursor()

        try:
            if not self.dataframe_to_closed.empty:
                self.log.info(f'Добавляем строки удаленных вакансий в таблицу {self.table_name}.')
                data_tuples_to_closed = [tuple(x) for x in self.dataframe_to_closed.to_records(index=False)]
                cols = ",".join(self.dataframe_to_closed.columns)
                query = f"""INSERT INTO {self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 13)})"""
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк удалено из core_fact_table: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица {self.table_name} в БД "
                              f"{config['database']}.")

                self.log.info(f'Вставляем строки удаленных вакансий в таблицу del_vacancy_core.')
                query = f"""INSERT INTO del_vacancy_core ({cols}) VALUES ({", ".join(["%s"] * 13)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк вставлено в del_vacancy_core: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица del_vacancy_core в БД "
                              f"{config['database']}.")

                self.log.info(f'Удаляем из core_fact_table закрытые вакансии.')
                data_to_delete_tuples = [tuple(x) for x in
                                         self.dataframe_to_closed[['link']].to_records(index=False)]
                for to_delete in data_to_delete_tuples:
                    query = f"""DELETE FROM core_fact_table WHERE link = '{to_delete[0]}'"""
                    self.log.info(f"Запрос вставки данных: {query}")
                    self.cur.executemany(query, data_to_delete_tuples)
                    self.log.info(f"Количество строк удалено из core_fact_table: "
                                  f"{len(data_to_delete_tuples)}, обновлена таблица core_fact_table в БД "
                                  f"{config['database']}.")

            else:
                self.log.info(f"dataframe_to_closed пуст.")

            if not self.dataframe_to_update.empty:
                self.log.info(f'Обновляем таблицу {self.table_name}.')
                data_tuples_to_insert = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                cols = ",".join(self.dataframe_to_update.columns)
                query = f"""INSERT INTO {self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 12)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_insert)
                self.log.info(f"Количество строк вставлено в {self.table_name}: "
                              f"{len(data_tuples_to_insert)}, обновлена таблица {self.table_name} "
                              f"в БД {config['database']}.")

                self.log.info(f'Обновляем таблицу core_fact_table.')
                core_fact_data_tuples = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                query = f"""INSERT INTO core_fact_table ({cols}) VALUES ({", ".join(["%s"] * 12)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, core_fact_data_tuples)
                self.log.info(f"Количество строк вставлено в core_fact_table: "
                              f"{len(core_fact_data_tuples)}, обновлена таблица core_fact_table "
                              f"в БД {config['database']}.")
            else:
                self.log.info(f"dataframe_to_update пуст.")

            self.conn.commit()
            self.log.info(f"Операции успешно выполнены. Изменения сохранены в таблицах.")
        except Exception as e:
            self.conn.rollback()
            self.log.error(f"Произошла ошибка: {str(e)}")
        finally:
            self.cur.close()

class YandJobParser(BaseJobParser):
    def find_vacancies(self):
        self.log.info('Старт парсинга вакансий Yandex')
        self.df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'company', 'description', 'skills', 'source_vac',
                                        'date_created', 'date_of_download', 'status', 'version_vac', 'actual'])
        self.log.info("Создан DataFrame для записи вакансий")
        self.browser.implicitly_wait(3)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs:
            text_str = self.url + '?text=' + str(prof['fullName'].replace(' ', '+')).lower()
            self.browser.get(text_str)
            self.browser.maximize_window()
            self.browser.delete_all_cookies()
            self.browser.implicitly_wait(10)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                # Подсчет количества предложений
                self.browser.implicitly_wait(60)
                vacs_bar = self.browser.find_element(By.CLASS_NAME, 'lc-jobs-vacancies-list')
                vacs = vacs_bar.find_elements(By.CLASS_NAME, 'lc-jobs-vacancy-card')
                self.log.info(f"Парсим вакансии по запросу: {prof['fullName']}")
                self.log.info(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    try:
                        vac_info = {}
                        find_vacancy_id = vac.find_element(By.CLASS_NAME, 'lc-jobs-vacancy-card__link')
                        vac_info['vacancy_id'] = find_vacancy_id.get_attribute('href')
                        vac_info['company'] = vac.find_elements(By.CLASS_NAME, 'lc-styled-text')[0].text
                        self.df.loc[len(self.df)] = vac_info

                    except Exception as e:
                        self.log.error(f"Произошла ошибка: {e}")
                        continue

            except Exception as e:
                self.log.error(f"Ошибка {e}")

        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий в Yandex после удаления дубликатов: " +
                      str(len(self.df)) + "\n")
        self.df['description'] = None
        # self.df['version_vac'] = 1

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для YandJobParser.
        """
        if not self.df.empty:
            self.log.info('Старт парсинга описаний вакансий')
            for descr in self.df.index:
                try:
                    vacancy_id = self.df.loc[descr, 'vacancy_id']
                    self.browser.get(vacancy_id)
                    self.browser.delete_all_cookies()
                    self.browser.implicitly_wait(5)
                    if isinstance(self, YandJobParser):
                        desc = self.browser.find_element(By.CLASS_NAME, 'lc-jobs-vacancy-mvp__description').text
                        desc = desc.replace(';', '')
                        self.df.loc[descr, 'description'] = str(desc)
                        skills = self.browser.find_element(By.CLASS_NAME, 'lc-jobs-tags-block').text
                        self.df.loc[descr, 'skills'] = str(skills)
                        header = self.browser.find_element(By.CLASS_NAME, 'lc-jobs-content-header')
                        vacancy_name = header.find_element(By.CLASS_NAME, 'lc-styled-text__text').text
                        self.df.loc[descr, 'vacancy_name'] = str(vacancy_name)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'vacancy_id']}")
                    pass
        else:
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Yandex
        """
        self.cur = self.conn.cursor()
        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        try:
            if not self.df.empty:
                self.cur = self.conn.cursor()
                self.df['date_created'] = datetime.now().date()
                self.df['date_of_download'] = datetime.now().date()
                self.df['source_vac'] = url_yand
                self.df['status'] = 'existing'
                self.df['actual'] = 1
                self.table_name = raw_tables[3]
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")

                self.log.info('Собираем вакансии для сравнения')
                query = f"""SELECT vacancy_id FROM {self.table_name}
                            WHERE version_vac = (SELECT max(version_vac) FROM {self.table_name})
                            ORDER BY date_of_download DESC, version_vac DESC LIMIT 1"""
                self.cur.execute(query)
                links_in_db = self.cur.fetchall()
                links_in_db_set = set(vacancy_id for vacancy_id, in links_in_db)
                links_in_parsed = set(self.df['vacancy_id'])
                links_to_close = links_in_db_set - links_in_parsed

                self.dataframe_to_closed = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'company', 'description', 'skills', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'date_closed', 'version_vac', 'actual'
                ])

                self.dataframe_to_update = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'company', 'description', 'skills', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'version_vac', 'actual'
                ])

                self.log.info('Создаем датафрейм dataframe_to_closed')
                if links_to_close:
                    for link in links_to_close:
                        query = f"""
                            SELECT vacancy_id, vacancy_name, company, description, skills, source_vac,
                                   date_created, date_of_download, status, date_closed, version_vac, actual
                            FROM {self.table_name}
                            WHERE vacancy_id = '{link}'
                                AND status != 'closed'
                                AND actual != '-1'
                                AND version_vac = (
                                    SELECT max(version_vac) FROM {self.table_name}
                                    WHERE vacancy_id = '{link}'
                                )
                            ORDER BY date_of_download DESC, version_vac DESC
                            LIMIT 1
                            """
                        self.cur.execute(query)
                        records_to_close = self.cur.fetchall()
                        if records_to_close:
                            for record in records_to_close:
                                data = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'company': record[2],
                                    'description': record[3], 'skills': record[4], 'source_vac': record[5],
                                    'date_created': record[6], 'date_of_download': datetime.now().date(),
                                    'status': 'closed', 'date_closed': datetime.now().date(),
                                    'version_vac': record[-2] + 1, 'sign': -1
                                }
                                self.dataframe_to_closed = pd.concat([self.dataframe_to_closed,
                                                                      pd.DataFrame(data, index=[0])])
                    self.log.info('Датафрейм dataframe_to_closed создан')
                else:
                    self.log.info('Список links_to_close пуст')

                self.log.info('Присваиваем статусы изменений')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    link = record[0]
                    query = f"""
                        SELECT vacancy_id, vacancy_name, company, description, skills, source_vac,
                               date_created, date_of_download, status, version_vac, actual
                        FROM {self.table_name}
                        WHERE vacancy_id = '{link}'
                        ORDER BY date_of_download DESC, version_vac DESC
                        LIMIT 1
                        """
                    self.cur.execute(query)
                    records_in_db = self.cur.fetchall()
                    if records_in_db:
                        for old_record in records_in_db:
                            old_status = old_record[-3]
                            next_version = old_record[-2] + 1
                            if old_status == 'new':
                                data_new_vac = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'company': record[2],
                                    'description': record[3], 'skills': record[4], 'source_vac': record[5],
                                    'date_created': old_record[6], 'date_of_download': datetime.now().date(),
                                    'status': 'existing', 'version_vac': next_version, 'actual': 1
                                }
                                self.dataframe_to_update = pd.concat(
                                    [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                )
                            elif old_status == 'existing':
                                if old_record[1] == record[1] and old_record[2] == record[2] and \
                                    old_record[3] == record[3] and old_record[4] == record[4] and \
                                    old_record[5] == record[5] and old_record[6] == record[6]:
                                    pass
                                else:
                                    data_new_vac = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'company': record[2],
                                        'description': record[3], 'skills': record[4], 'source_vac': record[5],
                                        'date_created': old_record[6], 'date_of_download': datetime.now().date(),
                                        'status': 'existing', 'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
                                    )
                            elif old_status == 'closed':
                                if link in links_in_parsed:
                                    data_clos_new = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'company': record[2],
                                        'description': record[3], 'skills': record[4], 'source_vac': record[5],
                                        'date_created': record[6], 'date_of_download': datetime.now().date(),
                                        'status': 'new', 'version_vac': next_version, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_clos_new, index=[0])]
                                    )
                                else:
                                    data_full_new = {
                                        'vacancy_id': link, 'vacancy_name': record[1], 'company': record[2],
                                        'description': record[3], 'skills': record[4], 'source_vac': record[5],
                                        'date_created': record[6], 'date_of_download': datetime.now().date(),
                                        'status': 'new', 'version_vac': 1, 'actual': 1
                                    }
                                    self.dataframe_to_update = pd.concat(
                                        [self.dataframe_to_update, pd.DataFrame(data_full_new, index=[0])]
                                    )

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise

    def update_database_queries(self):
        """
        Метод для выполнения запросов к базе данных.
        """
        self.cur = self.conn.cursor()

        try:
            if not self.dataframe_to_closed.empty:
                self.log.info(f'Добавляем строки удаленных вакансий в таблицу {self.table_name}.')
                data_tuples_to_closed = [tuple(x) for x in self.dataframe_to_closed.to_records(index=False)]
                cols = ",".join(self.dataframe_to_closed.columns)
                query = f"""INSERT INTO {self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 12)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк удалено из core_fact_table: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица {self.table_name} в БД "
                              f"{config['database']}.")

                self.log.info(f'Вставляем строки удаленных вакансий в таблицу del_vacancy_core.')
                query = f"""INSERT INTO del_vacancy_core ({cols}) VALUES ({", ".join(["%s"] * 12)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_closed)
                self.log.info(f"Количество строк вставлено в del_vacancy_core: "
                              f"{len(data_tuples_to_closed)}, обновлена таблица del_vacancy_core в БД "
                              f"{config['database']}.")

                self.log.info(f'Удаляем из core_fact_table закрытые вакансии.')
                data_to_delete_tuples = [tuple(x) for x in
                                         self.dataframe_to_closed[['link']].to_records(index=False)]
                for to_delete in data_to_delete_tuples:
                    query = f"""DELETE FROM core_fact_table WHERE link = '{to_delete[0]}'"""
                    self.log.info(f"Запрос вставки данных: {query}")
                    self.cur.executemany(query, data_to_delete_tuples)
                    self.log.info(f"Количество строк удалено из core_fact_table: "
                                  f"{len(data_to_delete_tuples)}, обновлена таблица core_fact_table в БД "
                                  f"{config['database']}.")

            else:
                self.log.info(f"dataframe_to_closed пуст.")

            if not self.dataframe_to_update.empty:
                data_tuples_to_insert = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                cols = ",".join(self.dataframe_to_update.columns)
                self.log.info(f'Обновляем таблицу {self.table_name}.')
                query = f"""INSERT INTO {self.table_name} ({cols}) VALUES ({", ".join(["%s"] * 11)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, data_tuples_to_insert)
                self.log.info(f"Количество строк вставлено в {self.table_name}: "
                              f"{len(data_tuples_to_insert)}, обновлена таблица {self.table_name} "
                              f"в БД {config['database']}.")

                self.log.info(f'Обновляем таблицу core_fact_table.')
                core_fact_data_tuples = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
                query = f"""INSERT INTO core_fact_table ({cols}) VALUES ({", ".join(["%s"] * 11)})"""
                self.log.info(f"Запрос вставки данных: {query}")
                self.cur.executemany(query, core_fact_data_tuples)
                self.log.info(f"Количество строк вставлено в core_fact_table: "
                              f"{len(core_fact_data_tuples)}, обновлена таблица core_fact_table "
                              f"в БД {config['database']}.")
            else:
                self.log.info(f"dataframe_to_update пуст.")

            self.conn.commit()
            self.log.info(f"Операции успешно выполнены. Изменения сохранены в таблицах.")
        except Exception as e:
            self.conn.rollback()
            self.log.error(f"Произошла ошибка: {str(e)}")
        finally:
            self.cur.close()

def run_vk_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий VK
    """
    log = context['ti'].log
    log.info('Запуск парсера ВК')
    try:
        parser = VKJobParser(url_vk, profs, log, conn)
        parser.find_vacancies()
        parser.find_vacancies_description()
        parser.save_df()
        parser.update_database_queries()
        parser.stop()
        log.info('Парсер ВК успешно провел работу')
    except Exception as e_outer:
        log.error(f'Исключение в функции run_vk_parser: {e_outer}')

def run_sber_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий Sber
    """
    log = context['ti'].log
    log.info('Запуск парсера Сбербанка')
    try:
        parser = SberJobParser(url_sber, profs, log, conn)
        parser.find_vacancies()
        parser.find_vacancies_description()
        parser.save_df()
        parser.update_database_queries()
        parser.stop()
        log.info('Парсер Сбербанка успешно провел работу')
    except Exception as e_outer:
        log.error(f'Исключение в функции run_sber_parser: {e_outer}')

def run_tin_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий Tinkoff
    """
    log = context['ti'].log
    log.info('Запуск парсера Тинькофф')
    try:
        parser = TinkoffJobParser(url_tin, profs, log, conn)
        parser.open_all_pages()
        parser.all_vacs_parser()
        parser.find_vacancies_description()
        parser.save_df()
        parser.update_database_queries()
        parser.stop()
        log.info('Парсер Тинькофф успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Тинькофф: {e}')

def run_yand_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий Yandex
    """
    log = context['ti'].log
    log.info('Запуск парсера Yandex')
    try:
        parser = YandJobParser(url_yand, profs, log, conn)
        parser.find_vacancies()
        parser.find_vacancies_description()
        parser.save_df()
        parser.update_database_queries()
        parser.stop()
        log.info('Парсер Yandex успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Yandex: {e}')


hello_bash_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"')


parse_vkjobs = PythonOperator(
    task_id='parse_vkjobs',
    python_callable=run_vk_parser,
    provide_context=True,
    dag=updated_raw_dag
)

parse_sber = PythonOperator(
    task_id='parse_sber',
    python_callable=run_sber_parser,
    provide_context=True,
    dag=updated_raw_dag
)

parse_tink = PythonOperator(
    task_id='parse_tink',
    python_callable=run_tin_parser,
    provide_context=True,
    dag=updated_raw_dag
)

parse_yand = PythonOperator(
    task_id='parse_yand',
    python_callable=run_yand_parser,
    provide_context=True,
    dag=updated_raw_dag
)

end_task = DummyOperator(
    task_id="end_task"
)

# hello_bash_task >> parse_tink >> end_task
hello_bash_task >> parse_vkjobs >> parse_sber >> parse_tink >> parse_yand >> end_task