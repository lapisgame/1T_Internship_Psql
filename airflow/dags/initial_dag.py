# from clickhouse_driver import Client
import dateparser
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv, json
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from airflow import DAG
from selenium.common.exceptions import NoSuchElementException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import logging
from logging import handlers
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

# # Connections settings
# # Загружаем данные подключений из JSON файла
# with open('/opt/airflow/dags/config_connections.json', 'r') as conn_file:
#     connections_config = json.load(conn_file)
#
# # Получаем данные конфигурации подключения и создаем конфиг для клиента
# conn_config = connections_config['psql_connect']
#
# config = {
#     'database': conn_config['database'],
#     'user': conn_config['user'],
#     'password': conn_config['password'],
#     'host': conn_config['host'],
#     'port': conn_config['port'],
# }
#
# client = psycopg2.connect(**config)
#
# # Variables settings
# # Загружаем переменные из JSON файла
# with open('/opt/airflow/dags/config_variables.json', 'r') as config_file:
#     my_variables = json.load(config_file)
#
# # Проверяем, существует ли переменная с данным ключом
# if not Variable.get("shares_variable", default_var=None):
#     # Если переменная не существует, устанавливаем ее
#     Variable.set("shares_variable", my_variables, serialize_json=True)
#
# dag_variables = Variable.get("shares_variable", deserialize_json=True)

# Загружаем переменные из JSON файла
with open('/opt/airflow/dags/config_variables.json', 'r') as config_file:
    my_variables = json.load(config_file)

dag_variables = Variable.set("shares_variable", my_variables, serialize_json=True)

# Получение значений из переменной окружения.
dag_variables = Variable.get('shares_variable', deserialize_json=True)


# Получение объекта Connection с помощью метода BaseHook.get_connection
def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """
    Function returns dictionary with connection credentials

    :param conn_id: str with airflow connection id
    :return: Connection
    """
    conn = BaseHook.get_connection(conn_id)
    return conn


# Получаем данные соединения с базой данных из переменных DAG
pg_conn = get_conn_credentials(dag_variables.get('conn_id'))
# Извлекаем параметры соединения с базой данных
pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port, pg_conn.login, pg_conn.password, pg_conn.schema


# Создаем подключение к базе данных PostgreSQL с помощью полученных параметров
client = psycopg2.connect(
    host=pg_hostname,
    port=pg_port,
    user=pg_username,
    password=pg_pass,
    database=pg_db,
    options=dag_variables.get('options')
)


url_sber = dag_variables.get('base_sber')
url_yand = dag_variables.get('base_yand')
url_vk = dag_variables.get('base_vk')
url_tin = dag_variables.get('base_tin')

raw_tables = ['raw_vk', 'raw_sber', 'raw_tin']

options = ChromeOptions()

profs = dag_variables['professions']

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

# Создаем DAG ручного запуска (инициализирующий режим).
initial_dag = DAG(dag_id='initial_dag',
                tags=['admin_1T'],
                start_date=datetime(2023, 10, 29),
                schedule_interval=None,
                default_args=default_args
                )

class DatabaseManager:
    def __init__(self, client):
        self.client = client
        self.cur = client.cursor()
        self.raw_tables = ['raw_vk', 'raw_sber', 'raw_tin']
        self.log = LoggingMixin().log

    def create_raw_tables(self):
        for table_name in self.raw_tables:
            try:
                drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
                self.cur.execute(drop_table_query)
                self.log.info(f'Удалена таблица {table_name}')

                create_table_query = f"""                
                CREATE TABLE {table_name}(
                   link VARCHAR(255) NOT NULL,
                   vacancy_name VARCHAR(100),
                   locat_work VARCHAR(255),
                   level VARCHAR(255),
                   company VARCHAR(255),
                   salary_from BIGINT,
                   salary_to BIGINT,
                   exp_from SMALLINT,
                   exp_to SMALLINT,
                   description TEXT,
                   job_type VARCHAR(255),
                   job_format VARCHAR(255),
                   lang VARCHAR(255),
                   skills VARCHAR(255),
                   source_vac VARCHAR(255),
                   date_created DATE,
                   date_of_download DATE NOT NULL, 
                   status VARCHAR(32),
                   date_closed DATE,
                   version_vac INTEGER NOT NULL,
                   actual SMALLINT,
                   PRIMARY KEY(link, version_vac)
                );
                """
                self.cur.execute(create_table_query)
                self.client.commit()
                self.log.info(f'Таблица {table_name} создана в базе данных.')
            except Exception as e:
                self.log.error(f'Ошибка при создании таблицы {table_name}: {e}')
                self.client.rollback()

    def create_core_fact_table(self):
        try:
            table_name = 'core_fact_table'
            # self.cur = client.cursor()
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
            self.cur.execute(drop_table_query)
            self.log.info(f'Удалена таблица {table_name}')
            create_core_fact_table = f"""
            CREATE TABLE {table_name} AS
            (SELECT link, vacancy_name, locat_work, level, company, salary_from, salary_to, exp_from, exp_to,
                   description, job_type, job_format, lang, skills, source_vac, date_created, date_of_download,
                   status, date_closed, version_vac, actual
            FROM raw_vk
            UNION ALL
            SELECT link, vacancy_name, locat_work, level, company, salary_from, salary_to, exp_from, exp_to,
                   description, job_type, job_format, lang, skills, source_vac, date_created, date_of_download,
                   status, date_closed, version_vac, actual
            FROM raw_sber
            UNION ALL
            SELECT link, vacancy_name, locat_work, level, company, salary_from, salary_to, exp_from, exp_to,
                   description, job_type, job_format, lang, skills, source_vac, date_created, date_of_download,
                   status, date_closed, version_vac, actual
            FROM raw_tin);
            """
            self.cur.execute(create_core_fact_table)
            self.client.commit()
            # закрываем курсор и соединение с базой данных
            self.cur.close()
            self.client.close()
            self.log.info(f'Таблица {table_name} создана в базе данных.')
        except Exception as e:
            self.log.error(f'Ошибка при создании таблицы {table_name}: {e}')
            self.client.rollback()

class BaseJobParser:
    def __init__(self, url, profs, log, client):
        self.browser = webdriver.Remote(command_executor='http://selenium-router:4444/wd/hub', options=options)
        self.url = url
        self.browser.get(self.url)
        self.browser.maximize_window()
        self.browser.delete_all_cookies()
        time.sleep(2)
        self.profs = profs
        self.log = log
        self.client = client

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
        self.cur = self.client.cursor()
        self.df = pd.DataFrame(columns=['link', 'vacancy_name', 'locat_work', 'company', 'source_vac', 'date_created',
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
                    vac_info['link'] = str(vac.get_attribute('href'))
                    vac_info['vacancy_name'] = str(vac.find_element(By.CLASS_NAME, 'title-block').text)
                    vac_info['locat_work'] = str(vac.find_element(By.CLASS_NAME, 'result-item-place').text)
                    vac_info['company'] = str(vac.find_element(By.CLASS_NAME, 'result-item-unit').text)
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_button.clear()

        self.df = self.df.drop_duplicates()
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
                    link = self.df.loc[descr, 'link']
                    self.browser.get(link)
                    self.browser.delete_all_cookies()
                    time.sleep(3)
                    desc = self.browser.find_element(By.CLASS_NAME, 'section').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'link']}")
                    pass
        else:
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных vk
        """
        self.cur = self.client.cursor()

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        try:
            if not self.df.empty:
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
                table_name = 'raw_vk'
                # данные, которые вставляются в таблицу PosqtgreSQL
                data = [tuple(x) for x in self.df.to_records(index=False)]
                # формируем строку запроса с плейсхолдерами для значений
                query = f"INSERT INTO {table_name} (link, vacancy_name, locat_work, company, source_vac, " \
                        f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
                # исполняем запрос с использованием execute_values
                self.log.info(f"Запрос вставки данных: {query}")
                self.log.info(f"Данные для вставки: {data}")
                execute_values(self.cur, query, data)

                self.client.commit()
                # закрываем курсор и соединение с базой данных
                self.cur.close()
                self.client.close()
                # логируем количество обработанных вакансий
                self.log.info("Общее количество загруженных в БД вакансий после удаления дубликатов: "
                              + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise

class SberJobParser(BaseJobParser):
    """
    Парсер для вакансий с сайта Sberbank, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с Sberbank
        """
        self.cur = self.client.cursor()
        self.df = pd.DataFrame(columns=['link', 'vacancy_name', 'locat_work', 'company', 'source_vac', 'date_created',
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
                    vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    data = vac.find_elements(By.CLASS_NAME, 'Text-sc-36c35j-0')
                    vac_info['vacancy_name'] = data[0].text
                    vac_info['locat_work'] = data[2].text
                    vac_info['company'] = data[3].text
                    vac_info['date_created'] = data[4].text
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_str.clear()

        # Удаление дубликатов в DataFrame
        self.df = self.df.drop_duplicates()
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
            for descr in self.df.index:
                try:
                    link = self.df.loc[descr, 'link']
                    self.browser.get(link)
                    self.browser.delete_all_cookies()
                    self.browser.implicitly_wait(3)
                    desc = self.browser.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[3]'
                                                               '/div/div/div[3]/div[3]').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'link']}")
        else:
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Sber
        """
        self.cur = self.client.cursor()

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

                table_name = 'raw_sber'
                # данные, которые вставляются в таблицу PosqtgreSQL
                data = [tuple(x) for x in self.df.to_records(index=False)]

                query = f"INSERT INTO {table_name} (link, vacancy_name, locat_work, company, source_vac, " \
                        f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
                # исполняем запрос с использованием execute_values
                self.log.info(f"Запрос вставки данных: {query}")
                self.log.info(f"Данные для вставки: {data}")
                execute_values(self.cur, query, data)

                self.client.commit()
                # закрываем курсор и соединение с базой данных
                self.cur.close()
                self.client.close()
                # логируем количество обработанных вакансий
                self.log.info("Общее количество загруженных в БД вакансий после удаления дубликатов: "
                              + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных в raw-слой Sber {e}")


class TinkoffJobParser(BaseJobParser):
    """
    Парсер вакансий с сайта Tinkoff, наследованный от BaseJobParser
    """
    def open_all_pages(self):
        elements = self.browser.find_elements(By.CLASS_NAME, 'fuBQPo')
        for element in elements:
            element.click()

    def all_vacs_parser(self):
        """
        Метод для нахождения вакансий с Tinkoff
        """
        self.cur = self.client.cursor()

        self.df = pd.DataFrame(
            columns=['link', 'vacancy_name', 'locat_work', 'level', 'company', 'source_vac', 'date_created',
                     'date_of_download', 'status', 'version_vac', 'actual', 'description'])
        self.log.info("Создан DataFrame для записи вакансий")

        self.browser.implicitly_wait(3)
        try:
            vac_index = 0
            while True:
                try:
                    vac = self.browser.find_elements(By.CLASS_NAME, 'eM3bvP')[vac_index]
                except IndexError:
                    break  # Закончили обработку всех элементов

                self.log.info(f"Обработка вакансии номер {vac_index + 1}")

                vac_info = {}
                vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                data = vac.find_elements(By.CLASS_NAME, 'gM3bvP')
                vac_info['vacancy_name'] = data[0].text
                vac_info['level'] = data[1].text
                vac_info['locat_work'] = data[2].text
                self.df.loc[len(self.df)] = vac_info
                vac_index += 1

            self.df = self.df.drop_duplicates()
            self.df['company'] = 'Тинькофф'
            self.df['date_created'] = datetime.now().date()
            self.df['date_of_download'] = datetime.now().date()
            self.df['source_vac'] = url_tin
            self.df['description'] = None
            self.df['status'] = 'existing'
            self.df['actual'] = 1
            self.df['version_vac'] = 1

            self.log.info(
                f"Парсер завершил работу. Обработано {vac_index} вакансий. Оставлены только уникальные записи. "
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
                    link = self.df.loc[descr, 'link']
                    self.browser.get(link)
                    self.browser.delete_all_cookies()
                    self.browser.implicitly_wait(3)
                    desc = str(self.browser.find_element(By.CLASS_NAME, 'dyzaXu').text)
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = desc

                    self.log.info(f"Описание успешно добавлено для вакансии {descr + 1}")

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'link']}")
                    pass
        else:
            self.log.info(f"Нет описания вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Tinkoff
        """
        self.cur = self.client.cursor()

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
                table_name = 'raw_tin'
                data = [tuple(x) for x in self.df.to_records(index=False)]

                query = f"INSERT INTO {table_name} (link, vacancy_name, locat_work, level, company, source_vac, " \
                        f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
                # исполняем запрос с использованием execute_values
                self.log.info(f"Запрос вставки данных: {query}")
                self.log.info(f"Данные для вставки: {data}")
                execute_values(self.cur, query, data)

                self.client.commit()
                # закрываем курсор и соединение с базой данных
                self.cur.close()
                self.client.close()
                # логируем количество обработанных вакансий
                self.log.info("Общее количество загруженных в БД вакансий после удаления дубликатов: "
                              + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных в raw-слой Tinkoff {e}")


db_manager = DatabaseManager(client=client)


def run_vk_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий VK
    """
    log = context['ti'].log
    log.info('Запуск парсера ВК')
    try:
        parser = VKJobParser(url_vk, profs, log, client)
        parser.find_vacancies()
        parser.find_vacancies_description()
        parser.save_df()
        parser.stop()
        log.info('Парсер ВК успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера ВК: {e}')

def run_sber_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий Sber
    """
    log = context['ti'].log
    log.info('Запуск парсера Сбербанка')
    try:
        parser = SberJobParser(url_sber, profs, log, client)
        parser.find_vacancies()
        parser.find_vacancies_description()
        parser.save_df()
        parser.stop()
        log.info('Парсер Сбербанка успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Сбербанка: {e}')

def run_tin_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий Tinkoff
    """
    log = context['ti'].log
    log.info('Запуск парсера Тинькофф')
    try:
        parser = TinkoffJobParser(url_tin, profs, log, client)
        parser.open_all_pages()
        parser.all_vacs_parser()
        parser.find_vacancies_description()
        parser.save_df()
        parser.stop()
        log.info('Парсер Тинькофф успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера Тинькофф: {e}')


hello_bash_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"')

# Определение задачи
create_raw_tables = PythonOperator(
    task_id='create_raw_tables',
    python_callable=db_manager.create_raw_tables,
    provide_context=True,
    dag=initial_dag
)

parse_vkjobs = PythonOperator(
    task_id='parse_vkjobs',
    python_callable=run_vk_parser,
    provide_context=True,
    dag=initial_dag
)

parse_sber = PythonOperator(
    task_id='parse_sber',
    python_callable=run_sber_parser,
    provide_context=True,
    dag=initial_dag
)

parse_tink = PythonOperator(
    task_id='parse_tink',
    python_callable=run_tin_parser,
    provide_context=True,
    dag=initial_dag
)

create_core_fact_table = PythonOperator(
    task_id='create_core_fact_table',
    python_callable=db_manager.create_core_fact_table,
    provide_context=True,
    dag=initial_dag
)

end_task = DummyOperator(
    task_id="end_task"
)

hello_bash_task >> create_raw_tables >> parse_vkjobs >> parse_sber >> parse_tink >> create_core_fact_table >> end_task