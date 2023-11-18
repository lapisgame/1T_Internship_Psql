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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
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
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import requests
import warnings

warnings.filterwarnings('ignore')


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
url_remote =dag_variables.get('base_remote')

raw_tables = ['raw_remote']

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

# Создаем DAG ручного запуска (инициализирующий режим).
initial_dag = DAG(dag_id='initial_dag',
                tags=['admin_1T'],
                start_date=datetime(2023, 11, 12),
                schedule_interval=None,
                default_args=default_args
                )

class DatabaseManager:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.raw_tables = raw_tables
        self.log = LoggingMixin().log

    def create_raw_tables(self):
        table_name = 'raw_remote'
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
               PRIMARY KEY(vacancy_id, version_vac)
            );
            """
            self.cur.execute(create_table_query)
            self.conn.commit()
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


class RemoteJobParser(BaseJobParser):
    """
    Парсер вакансий с сайта Remote_Job, наследованный от BaseJobParser
    """
    def main_page(self, url):
        self.log.info(f'Анализируется главная страница {url}')
        self.browser.get(url)
        time.sleep(3)
        divs = self.browser.find_elements(By.CSS_SELECTOR, '.col-xs-10')

        for div in divs:
            vacancy_data = {}

            salary_info = div.find_element(By.TAG_NAME, 'h3').text
            if salary_info == "з.п. не указана":
                salary_from = None
                salary_to = None
            else:
                try:
                    cleaned_salary_info = salary_info.replace(' ', '').replace('руб.', '')
                    if 'от' in cleaned_salary_info and 'до' in cleaned_salary_info:
                        salary_parts = list(map(int, cleaned_salary_info.split('от')[1].split('до')))
                        salary_from = salary_parts[0]
                        salary_to = salary_parts[1]
                    elif 'от' in cleaned_salary_info:
                        salary_from = int(cleaned_salary_info.split('от')[-1])
                        salary_to = None
                    elif 'до' in cleaned_salary_info:
                        salary_from = None
                        salary_to = int(cleaned_salary_info.split('до')[-1])
                    else:
                        salary_from = None
                        salary_to = None
                except Exception as e:
                    self.log.error(f"Ошибка при обработке информации о зарплате: {e}")
                    salary_from = None
                    salary_to = None

            vacancy_link = div.find_element(By.TAG_NAME, 'a').get_attribute('href')
            vacancy_name = div.find_element(By.CSS_SELECTOR, '.navbar li, a, button').text
            company = div.find_elements(By.TAG_NAME, 'small')[1].text.strip()

            vacancy_data['vacancy_link'] = vacancy_link
            vacancy_data['vacancy_name'] = vacancy_name
            vacancy_data['company'] = company
            vacancy_data['salary_from'] = salary_from
            vacancy_data['salary_to'] = salary_to

            self.url_l.append(vacancy_data)

    def find_vacancies_description(self):
        # инициализация главного словаря с данными
        for vacancy in self.url_l:
            # self.log.info(f'URL {vacancy["vacancy_link"]}')
            self.browser.get(vacancy["vacancy_link"])
            try:
                date_created = dateparser.parse(
                    self.wait.until(EC.presence_of_element_located((By.XPATH, "//*[@class='text-left']"))).text.strip(),
                    languages=['ru']).date()
            except:
                date_created = datetime.now().date()

            try:
                text_tag = self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@class="row p-y-3"]')))
                text = "\n".join((text_tag.text.strip().split("\n")[2:])).replace('\r', '')
                description = text[:text.find('Откликнуться на вакансию')].strip().replace(
                    'Контактная информация работодателя станет доступна сразу после того, как вы оставите свой отклик на эту вакансию.',
                    '')
            except TimeoutException:
                # Если исключение вызвано, пропустить текущую итерацию и перейти к следующей вакансии.
                self.log.error(
                    f"Не удалось найти текстовый элемент на странице {vacancy['vacancy_link']}. Страница будет пропущена.")
                continue

            self.df['vacancy_id'].append(vacancy["vacancy_link"])
            self.df['vacancy_name'].append(vacancy["vacancy_name"])
            self.df['company'].append(vacancy["company"])
            self.df['salary_from'].append(vacancy["salary_from"])
            self.df['salary_to'].append(vacancy["salary_to"])
            self.df['description'].append(description)
            self.df['job_format'].append('Удаленная работа')
            self.df['source_vac'].append('https://remote-job.ru/')
            self.df['date_created'].append(date_created)
            self.df['date_of_download'].append(datetime.now().date())
            self.df['status'].append('existing')
            self.df['version_vac'].append(1)
            self.df['actual'].append(1)
            time.sleep(3)

    def find_vacancies(self):
        self.wait = WebDriverWait(self.browser, 10)
        self.url_l = []
        self.df = {'vacancy_id': [], 'vacancy_name': [], 'company': [], 'salary_from': [], 'salary_to': [],
                   'description': [], 'job_format': [], 'source_vac': [], 'date_created': [],
                   'date_of_download': [], 'status': [], 'version_vac': [], 'actual': []
                   }
        options.add_argument('--headless')
        ua = UserAgent().chrome
        self.headers = {'User-Agent': ua}
        options.add_argument(f'--user-agent={ua}')

        self.log.info('Старт парсинга вакансий Remote-Job ...')

        for prof in self.profs:
            prof_name = prof['fullName']
            self.log.info(f'Старт парсинга вакансии: "{prof_name}"')
            try:
                self.browser.get(self.url)
                time.sleep(10)
                # операции поиска и обработки вакансий
            except Exception as e:
                self.log.error(f"Ошибка при обработке вакансии {prof_name}: {e}")
                continue
            try:
                search = self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="search_query"]')))
                search.send_keys(prof_name)
                search.send_keys(Keys.ENTER)
            except NoSuchElementException:
                self.log.error(f"No such element: Unable to locate element: for profession {prof_name}")
                continue

            if self.browser.find_element(By.CSS_SELECTOR, '.h2, h2').text == 'Vacancies not found':
                continue
            try:
                last_page = int(self.browser.find_element(By.CLASS_NAME, 'pagination').text.split('\n')[-2])
            except NoSuchElementException:
                last_page = 2

            vacancy_url = self.browser.current_url

            self.log.info(f'Страниц для обработки: {last_page}')

            for i in range(1, last_page + 1):
                self.log.info(f'Обрабатывается страница {i}/{last_page}.')
                vacancy_url_for_page = f'{vacancy_url}&page={i}'
                self.main_page(vacancy_url_for_page)
                self.find_vacancies_description()
                self.url_l = []
                self.log.info(f'Страница {i} обработана!')

            # Добавляем обновление браузера для каждой новой вакансии
            self.browser.refresh()
            time.sleep(5)  # Пауза после обновления страницы для уверенности, что страница прогрузилась полностью

    def save_df(self):
        self.df = pd.DataFrame(self.df)
        self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
        # self.df.reset_index(drop=True, inplace=True)
        self.df = self.df.drop_duplicates()
        self.log.info(
            "Всего найдено вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")

        self.log.info("Сохранение результатов в Базу Данных...")
        self.cur = self.conn.cursor()

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        try:
            if not self.df.empty:
                table_name = 'raw_remote'
                data = [tuple(x) for x in self.df.to_records(index=False)]
                query = f"INSERT INTO {table_name} (vacancy_id, vacancy_name, company, salary_from, salary_to, " \
                        f"description, job_format, source_vac, date_created, date_of_download, status, version_vac, " \
                        f"actual) VALUES %s"
                self.log.info(f"Запрос вставки данных: {query}")
                print(self.df.head())
                self.log.info(self.df.head())
                execute_values(self.cur, query, data)
                self.conn.commit()
                self.log.info("Общее количество загруженных в БД вакансий: " + str(len(self.df)) + "\n")

        except Exception as e:
            self.log.error(f"Произошла ошибка при сохранении данных в функции 'save_df': {e}")
            raise

        finally:
            self.cur.close()
            self.conn.close()

db_manager = DatabaseManager(conn=conn)

def init_run_remote_job_parser(**context):
    log = context['ti'].log
    log.info('Remote-job parser is starting')
    try:
        parser = RemoteJobParser('https://remote-job.ru/search?search%5Bquery%5D=&search%5BsearchType%5D=vacancy',
        profs, log, conn)
        parser.find_vacancies()
        parser.save_df()
        parser.stop()
        log.info('Parser remote-job has finished the job successfully')
    except Exception as e:
        log.error(f'An error occurred during the operation of the remote-job parser: {e}')


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


parse_remote_jobs = PythonOperator(
    task_id='parse_remote_jobs',
    python_callable=init_run_remote_job_parser,
    provide_context=True,
    dag=initial_dag
)

end_task = DummyOperator(
    task_id="end_task"
)

hello_bash_task >> create_raw_tables >> parse_remote_jobs >> end_task
