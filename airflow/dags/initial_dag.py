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
from typing import Callable
from airflow.utils.task_group import TaskGroup
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

raw_tables = ['raw_vk', 'raw_sber', 'raw_tinkoff', 'raw_yandex', 'del_vacancy_core']

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


class DatabaseManager:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.raw_tables = raw_tables
        self.log = LoggingMixin().log

    def create_raw_tables(self):
        for table_name in self.raw_tables:
            try:
                drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
                self.cur.execute(drop_table_query)
                self.log.info(f'Удалена таблица {table_name}')

                create_table_query = f"""                
                CREATE TABLE {table_name}(
                   vacancy_id VARCHAR(2083) NOT NULL,
                   vacancy_name VARCHAR(255),
                   towns VARCHAR(255),
                   level VARCHAR(255),
                   company VARCHAR(255),
                   salary_from DECIMAL(10, 2),
                   salary_to DECIMAL(10, 2),
                   exp_from DECIMAL(2, 1),
                   exp_to DECIMAL(2, 1),
                   description TEXT,
                   job_type VARCHAR(255),
                   job_format VARCHAR(255),
                   languages VARCHAR(255),
                   skills VARCHAR(511),
                   source_vac VARCHAR(255),
                   date_created DATE,
                   date_of_download DATE NOT NULL, 
                   status VARCHAR(32),
                   date_closed DATE,
                   version_vac INTEGER NOT NULL,
                   actual SMALLINT,
                   PRIMARY KEY(vacancy_id, version_vac)
                );
                """
                self.cur.execute(create_table_query)
                self.conn.commit()
                self.log.info(f'Таблица {table_name} создана в базе данных.')
            except Exception as e:
                self.log.error(f'Ошибка при создании таблицы {table_name}: {e}')
                self.conn.rollback()

    def create_core_fact_table(self):
        try:
            table_name = 'core_fact_table'
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
            self.cur.execute(drop_table_query)
            self.log.info(f'Удалена таблица {table_name}')
            create_core_fact_table = f"""
            CREATE TABLE {table_name} (
                vacancy_id VARCHAR(2083) NOT NULL,
                vacancy_name VARCHAR(255),
                towns VARCHAR(255),
                level VARCHAR(255),
                company VARCHAR(255),
                salary_from BIGINT,
                salary_to BIGINT,
                exp_from SMALLINT,
                exp_to SMALLINT,
                description TEXT,
                job_type VARCHAR(255),
                job_format VARCHAR(255),
                languages VARCHAR(255),
                skills VARCHAR(511),
                source_vac VARCHAR(255),
                date_created DATE,
                date_of_download DATE NOT NULL, 
                status VARCHAR(32),
                date_closed DATE,
                version_vac INTEGER NOT NULL,
                actual SMALLINT,
                PRIMARY KEY(vacancy_id)
            );"""
            self.cur.execute(create_core_fact_table)

            insert_query = f"""
            INSERT INTO {table_name}
                (SELECT vacancy_id, vacancy_name, towns, level, company, salary_from, salary_to, exp_from, exp_to,
                    description, job_type, job_format, languages, skills, source_vac, date_created, date_of_download,
                    status, date_closed, version_vac, actual
                FROM raw_vk
                UNION ALL
                SELECT vacancy_id, vacancy_name, towns, level, company, salary_from, salary_to, exp_from, exp_to,
                    description, job_type, job_format, languages, skills, source_vac, date_created, date_of_download,
                    status, date_closed, version_vac, actual
                FROM raw_sber
                UNION ALL
                SELECT vacancy_id, vacancy_name, towns, level, company, salary_from, salary_to, exp_from, exp_to,
                    description, job_type, job_format, languages, skills, source_vac, date_created, date_of_download,
                    status, date_closed, version_vac, actual
                FROM raw_tinkoff
                UNION ALL
                SELECT vacancy_id, vacancy_name, towns, level, company, salary_from, salary_to, exp_from, exp_to,
                    description, job_type, job_format, languages, skills, source_vac, date_created, date_of_download,
                    status, date_closed, version_vac, actual
                FROM raw_yandex);
            """
            self.cur.execute(insert_query)
            self.conn.commit()
            # закрываем курсор и соединение с базой данных
            self.cur.close()
            self.conn.close()
            self.log.info(f'Таблица {table_name} создана в базе данных.')
        except Exception as e:
            self.log.error(f'Ошибка при создании таблицы {table_name}: {e}')
            self.conn.rollback()

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


class VKJobParser(BaseJobParser):
    """
    Парсер вакансий с сайта VK, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с VK
        """
        self.cur = self.conn.cursor()
        self.df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'towns', 'company', 'source_vac', 'date_created',
                                        'date_of_download', 'status', 'version_vac', 'actual', 'description'])
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

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_button.clear()

        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")
        self.df['date_created'] = datetime.now().date()
        self.df['date_of_download'] = datetime.now().date()
        self.df['source_vac'] = url_vk
        self.df['status'] = 'existing'
        self.df['version_vac'] = 1
        self.df['actual'] = 1

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для VKJobParser.
        """
        self.log.info('Старт парсинга описаний вакансий')
        if not self.df.empty:
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
        self.cur = self.conn.cursor()

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        try:
            if not self.df.empty:
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
                table_name = raw_tables[0]
                # данные, которые вставляются в таблицу PosqtgreSQL
                data = [tuple(x) for x in self.df.to_records(index=False)]
                # формируем строку запроса с плейсхолдерами для значений
                query = f"INSERT INTO {table_name} (vacancy_id, vacancy_name, towns, company, source_vac, " \
                        f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
                # исполняем запрос с использованием execute_values
                self.log.info(f"Запрос вставки данных: {query}")
                execute_values(self.cur, query, data)

                self.conn.commit()
                # логируем количество обработанных вакансий
                self.log.info("Общее количество загруженных в БД вакансий: " + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise

        finally:
            # закрываем курсор и соединение с базой данных
            self.cur.close()
            self.conn.close()

class SberJobParser(BaseJobParser):
    """
    Парсер для вакансий с сайта Sberbank, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с Sberbank
        """
        self.cur = self.conn.cursor()
        self.df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'towns', 'company', 'source_vac', 'date_created',
                                        'date_of_download', 'status', 'version_vac', 'actual', 'description'])
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
        self.df['version_vac'] = 1
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
        self.cur = self.conn.cursor()

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        self.df['date_created'] = self.df['date_created'].dt.date

        try:
            if not self.df.empty:
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")

                table_name = raw_tables[1]
                # данные, которые вставляются в таблицу PosqtgreSQL
                data = [tuple(x) for x in self.df.to_records(index=False)]

                query = f"INSERT INTO {table_name} (vacancy_id, vacancy_name, towns, company, source_vac, " \
                        f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
                # исполняем запрос с использованием execute_values
                self.log.info(f"Запрос вставки данных: {query}")
                execute_values(self.cur, query, data)

                self.conn.commit()
                # логируем количество обработанных вакансий
                self.log.info("Общее количество загруженных в БД вакансий после удаления дубликатов: "
                              + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных в raw-слой Sber {e}")

        finally:
            # закрываем курсор и соединение с базой данных
            self.cur.close()
            self.conn.close()


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
            columns=['vacancy_id', 'vacancy_name', 'towns', 'level', 'company', 'source_vac', 'date_created',
                     'date_of_download', 'status', 'version_vac', 'actual', 'description'])
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
            self.df['date_created'] = datetime.now().date()
            self.df['date_of_download'] = datetime.now().date()
            self.df['source_vac'] = url_tin
            self.df['description'] = None
            self.df['status'] = 'existing'
            self.df['actual'] = 1
            self.df['version_vac'] = 1

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
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
                table_name = raw_tables[2]
                data = [tuple(x) for x in self.df.to_records(index=False)]

                query = f"INSERT INTO {table_name} (vacancy_id, vacancy_name, towns, level, company, source_vac, " \
                        f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
                # исполняем запрос с использованием execute_values
                self.log.info(f"Запрос вставки данных: {query}")
                execute_values(self.cur, query, data)

                self.conn.commit()
                # логируем количество обработанных вакансий
                self.log.info("Общее количество загруженных в БД вакансий: " + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных в raw-слой Tinkoff {e}")

        finally:
            # закрываем курсор и соединение с базой данных
            self.cur.close()
            self.conn.close()


class YandJobParser(BaseJobParser):
    def find_vacancies(self):
        self.log.info('Старт парсинга вакансий Yandex')
        self.df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'company', 'skills', 'source_vac', 'date_created',
                                        'date_of_download', 'status', 'version_vac', 'actual', 'description'])
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
        self.df['date_created'] = datetime.now().date()
        self.df['date_of_download'] = datetime.now().date()
        self.df['source_vac'] = url_yand
        self.df['description'] = None
        self.df['status'] = 'existing'
        self.df['actual'] = 1
        self.df['version_vac'] = 1

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
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
                table_name = raw_tables[3]
                # self.df.to_csv(f"./raw_data/{table_name}.csv", index=False, sep=';')
                # данные, которые вставляются в таблицу PosqtgreSQL
                data = [tuple(x) for x in self.df.to_records(index=False)]
                # формируем строку запроса с плейсхолдерами для значений
                query = f"INSERT INTO {table_name} (vacancy_id, vacancy_name, company, skills, source_vac, " \
                        f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
                # исполняем запрос с использованием execute_values
                self.log.info(f"Запрос вставки данных: {query}")
                execute_values(self.cur, query, data)

                self.conn.commit()
                # логируем количество обработанных вакансий
                self.log.info("Общее количество загруженных в БД вакансий: " + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise

        finally:
            # закрываем курсор и соединение с базой данных
            self.cur.close()
            self.conn.close()


db_manager = DatabaseManager(conn=conn)

def init_run_vk_parser(**context):
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
        parser.stop()
        log.info('Парсер ВК успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера ВК: {e}')

def init_run_sber_parser(**context):
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
        parser.stop()
        log.info('Парсер Сбербанка успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Сбербанка: {e}')

def init_run_tin_parser(**context):
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
        parser.stop()
        log.info('Парсер Тинькофф успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Тинькофф: {e}')


def init_run_yand_parser(**context):
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
        parser.stop()
        log.info('Парсер Yandex успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Yandex: {e}')


start_date = datetime(2023, 11, 8)

def generate_parser_task(task_id: str, run_parser: Callable):
    """
    Функция для создания оператора Python для запуска парсера.
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=run_parser,
        provide_context=True
    )

def generate_parsing_dag(dag_id: str, task_id: str, run_parser: Callable, start_date):
    """
    Функция для создания DAG с одной задачей парсинга
    """
    dag = DAG(
        dag_id=dag_id,
        default_args={
            "owner": "admin_1T",
            'retry_delay': timedelta(minutes=5),
        },
        start_date=start_date,
        # schedule_interval='@daily',
        schedule_interval=None,
    )

    with dag:
        hello_bash_task = BashOperator(
            task_id='hello_task',
            bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"'
        )

        parsed_task = generate_parser_task(task_id=task_id, run_parser=run_parser)

        end_task = DummyOperator(
            task_id="end_task"
        )

        hello_bash_task >> parsed_task >> end_task

    return dag

# db_manager = DatabaseManager(conn=conn)

initial_common_dag_id = 'initial_common_parsing_dag'

with DAG(
    dag_id=initial_common_dag_id,
    default_args={
        "owner": "admin_1T",
        'retry_delay': timedelta(minutes=5),
    },
    start_date=start_date,
    schedule_interval=None,
    ) as initial_common_dag:

    with TaskGroup('initial_parsers') as parsers:

        hello_bash_task = BashOperator(
            task_id='hello_task',
            bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"',
        )

        end_task = DummyOperator(
            task_id="end_task",
        )

        create_raw_tables = PythonOperator(
            task_id='create_raw_tables',
            python_callable=db_manager.create_raw_tables,
            provide_context=True,
        )

        create_core_fact_table = PythonOperator(
            task_id='create_core_fact_table',
            python_callable=db_manager.create_core_fact_table,
            provide_context=True,
        )

        with TaskGroup('parsers_group') as parsers_group:
            parse_vkjobs_task = generate_parser_task('parse_vkjobs', init_run_vk_parser)
            parse_sber_task = generate_parser_task('parse_sber', init_run_sber_parser)
            parse_tink_task = generate_parser_task('parse_tink', init_run_tin_parser)
            parse_yand_task = generate_parser_task('parse_yand', init_run_yand_parser)

            # Определение порядка выполнения задач внутри группы задач
            parse_vkjobs_task >> parse_sber_task >> parse_tink_task >> parse_yand_task

        hello_bash_task >> create_raw_tables >> parsers_group >> create_core_fact_table >> end_task

# Создаем отдельные DAG для каждой задачи парсинга
initial_dag_vk = generate_parsing_dag('initial_vk_parsing_dag', 'initial_parse_vkjobs',
                                      init_run_vk_parser, start_date)
initial_dag_sber = generate_parsing_dag('initial_sber_parsing_dag', 'initial_parse_sber',
                                        init_run_sber_parser, start_date)
initial_dag_tink = generate_parsing_dag('initial_tink_parsing_dag', 'initial_parse_tink',
                                        init_run_tin_parser, start_date)
initial_dag_yand = generate_parsing_dag('initial_yand_parsing_dag', 'initial_parse_yand',
                                        init_run_yand_parser, start_date)

# Делаем DAG's глобально доступными
globals()[initial_common_dag_id] = initial_common_dag
globals()[initial_dag_vk.dag_id] = initial_dag_vk
globals()[initial_dag_sber.dag_id] = initial_dag_sber
globals()[initial_dag_tink.dag_id] = initial_dag_tink
globals()[initial_dag_yand.dag_id] = initial_dag_yand

# hello_bash_task = BashOperator(
#     task_id='hello_task',
#     bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"')
#
# # Определение задачи
# create_raw_tables = PythonOperator(
#     task_id='create_raw_tables',
#     python_callable=db_manager.create_raw_tables,
#     provide_context=True,
#     dag=initial_dag
# )
#
# parse_vkjobs = PythonOperator(
#     task_id='parse_vkjobs',
#     python_callable=run_vk_parser,
#     provide_context=True,
#     dag=initial_dag
# )
#
# parse_sber = PythonOperator(
#     task_id='parse_sber',
#     python_callable=run_sber_parser,
#     provide_context=True,
#     dag=initial_dag
# )
#
# parse_tink = PythonOperator(
#     task_id='parse_tink',
#     python_callable=run_tin_parser,
#     provide_context=True,
#     dag=initial_dag
# )
#
# parse_yand = PythonOperator(
#     task_id='parse_yand',
#     python_callable=run_yand_parser,
#     provide_context=True,
#     dag=initial_dag
# )
#
# create_core_fact_table = PythonOperator(
#     task_id='create_core_fact_table',
#     python_callable=db_manager.create_core_fact_table,
#     provide_context=True,
#     dag=initial_dag
# )
#
# end_task = DummyOperator(
#     task_id="end_task"
# )
#
# hello_bash_task >> create_raw_tables >> parse_vkjobs >> parse_sber >> parse_tink >> parse_yand >> \
# create_core_fact_table >> end_task
