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


# profs = [i['fullName'] for i in dag_variables['professions']]
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


# class VKJobParser(BaseJobParser):
#     """
#     Парсер вакансий с сайта VK, наследованный от BaseJobParser
#     """
#     def find_vacancies(self):
#         """
#         Метод для нахождения вакансий с VK
#         """
#         self.cur = self.conn.cursor()
#         self.df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'towns', 'company', 'source_vac', 'date_created',
#                                         'date_of_download', 'status', 'version_vac', 'actual', 'description'])
#         self.log.info("Создан DataFrame для записи вакансий")
#         self.browser.implicitly_wait(3)
#         # Поиск и запись вакансий на поисковой странице
#         for prof in self.profs:
#             input_button = self.browser.find_element(By.XPATH,
#                                                      '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/div/div/div/input')
#             input_button.send_keys(prof['fullName'])
#             click_button = self.browser.find_element(By.XPATH,
#                                                      '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/button')
#             click_button.click()
#             time.sleep(5)
#
#             # Прокрутка вниз до конца страницы
#             self.scroll_down_page()
#
#             try:
#                 vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div[1]/div[2]/div/div')
#                 vacs = vacs_bar.find_elements(By.CLASS_NAME, 'result-item')
#                 vacs = [div for div in vacs if 'result-item' in str(div.get_attribute('class'))]
#                 self.log.info(f"Парсим вакансии по запросу: {prof['fullName']}")
#                 self.log.info(f"Количество: " + str(len(vacs)) + "\n")
#
#                 for vac in vacs:
#                     vac_info = {}
#                     vac_info['vacancy_id'] = str(vac.get_attribute('href'))
#                     vac_info['vacancy_name'] = str(vac.find_element(By.CLASS_NAME, 'title-block').text)
#                     vac_info['towns'] = str(vac.find_element(By.CLASS_NAME, 'result-item-place').text)
#                     vac_info['company'] = str(vac.find_element(By.CLASS_NAME, 'result-item-unit').text)
#                     self.df.loc[len(self.df)] = vac_info
#
#             except Exception as e:
#                 self.log.error(f"Произошла ошибка: {e}")
#                 input_button.clear()
#
#         self.df = self.df.drop_duplicates()
#         self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")
#         self.df['date_created'] = datetime.now().date()
#         self.df['date_of_download'] = datetime.now().date()
#         self.df['source_vac'] = url_vk
#         self.df['status'] = 'existing'
#         self.df['version_vac'] = 1
#         self.df['actual'] = 1
#
#     def find_vacancies_description(self):
#         """
#         Метод для парсинга описаний вакансий для VKJobParser.
#         """
#         self.log.info('Старт парсинга описаний вакансий')
#         if not self.df.empty:
#             for descr in self.df.index:
#                 try:
#                     vacancy_id = self.df.loc[descr, 'vacancy_id']
#                     self.browser.get(vacancy_id)
#                     self.browser.delete_all_cookies()
#                     time.sleep(3)
#                     desc = self.browser.find_element(By.CLASS_NAME, 'section').text
#                     desc = desc.replace(';', '')
#                     self.df.loc[descr, 'description'] = str(desc)
#
#                 except Exception as e:
#                     self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'vacancy_id']}")
#                     pass
#         else:
#             self.log.info(f"Нет вакансий для парсинга")
#
#     def save_df(self):
#         """
#         Метод для сохранения данных в базу данных vk
#         """
#         self.cur = self.conn.cursor()
#
#         def addapt_numpy_float64(numpy_float64):
#             return AsIs(numpy_float64)
#
#         def addapt_numpy_int64(numpy_int64):
#             return AsIs(numpy_int64)
#
#         register_adapter(np.float64, addapt_numpy_float64)
#         register_adapter(np.int64, addapt_numpy_int64)
#
#         try:
#             if not self.df.empty:
#                 self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
#                 table_name = raw_tables[0]
#                 # данные, которые вставляются в таблицу PosqtgreSQL
#                 data = [tuple(x) for x in self.df.to_records(index=False)]
#                 # формируем строку запроса с плейсхолдерами для значений
#                 query = f"INSERT INTO {table_name} (vacancy_id, vacancy_name, towns, company, source_vac, " \
#                         f"date_created, date_of_download, status, version_vac, actual, description) VALUES %s"
#                 # исполняем запрос с использованием execute_values
#                 self.log.info(f"Запрос вставки данных: {query}")
#                 execute_values(self.cur, query, data)
#
#                 self.conn.commit()
#                 # логируем количество обработанных вакансий
#                 self.log.info("Общее количество загруженных в БД вакансий: " + str(len(self.df)) + "\n")
#
#         except Exception as e:
#             self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
#             raise
#
#         finally:
#             # закрываем курсор и соединение с базой данных
#             self.cur.close()
#             self.conn.close()


# class RemoteJobParser(BaseJobParser):
#
#     def retrieve_page_data(self, url):
#         self.url_l = []
#
#         self.browser.implicitly_wait(3)
#         self.log.info("Создание DataFrame для записи вакансий")
#         self.df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'towns', 'company', 'salary_from', 'salary_to',
#                                         'exp_from', 'description', 'job_format', 'source_vac', 'date_created',
#                                         'date_of_download', 'status', 'version_vac', 'actual'])
#
#         try:
#             r = requests.get(url, headers=headers, timeout=10)
#         except:
#             self.log.error('не удалось получить доступ к странице')
#         soup = BeautifulSoup(r.content, 'html.parser')
#         div = soup.find_all('div', attrs={'class': 'col-xs-10 col-sm-10 col-md-10 col-lg-10 text-left'})
#         for i in div:
#             salary_info = ' '.join(i.find('h3').text.split())
#
#             if salary_info == "з.п. не указана":
#                 salary_from = None
#                 salary_to = None
#             else:
#                 try:
#                     salary_info = salary_info.replace(' ', '').replace('руб.', '')
#
#                     if 'от' in salary_info:
#                         salary_info = salary_info.split('от')[1]
#
#                     salary_parts = salary_info.split('до')
#
#                     if len(salary_parts) > 1:
#                         salary_from = int(salary_parts[0])
#                         salary_to = int(salary_parts[1])
#                     else:
#                         salary_from = int(salary_parts[0])
#                         salary_to = np.nan
#
#                 except Exception as e:
#                     self.log.error(f"Ошибка при обработке информации о зарплате: {e}")
#                     salary_from = np.nan
#                     salary_to = np.nan
#
#         vacancy = {'vacancy_id': 'https://remote-job.ru/' + i.find('a').get('href'),
#                    'vacancy_name': ' '.join(i.find('a').text.split()),
#                    'towns': None,
#                    'company': i.find_all('small')[1].text.strip(),
#                    'salary_from': salary_from,
#                    'salary_to': salary_to,
#                    'exp_from': np.nan,
#                    'description': None,
#                    'job_format': 'Удаленная работа',
#                    'source_vac': 'https://remote-job.ru/',
#                    'date_created': datetime.now().date(),
#                    'date_of_download': datetime.now().date(),
#                    'status': 'existing',
#                    'version_vac': 1,
#                    'actual': 1}
#
#         self.df = pd.concat([self.df, pd.DataFrame([vacancy])], ignore_index=True)
#         self.url_l.append('https://remote-job.ru/' + i.find('a').get('href'))
#
#     def find_vacancies_description(self):
#         self.log.info("Извлечение описаний вакансий...")
#         for url in self.url_l:
#             self.log.info("Обработка url: " + url)
#
#             try:
#                 r2 = requests.get(url, headers=headers, timeout=15)
#             except:
#                 self.log.error('Не удалось получить доступ к вакансии:')
#                 index = self.df[self.df['vacancy_id'] == url].index
#                 self.df = self.df.drop(index)
#                 continue
#
#             time.sleep(2)
#             r2_soup = BeautifulSoup(r2.content, 'html.parser')
#
#
#             b_elements = r2_soup.find_all('b')
#             if len(b_elements) > 4:
#                 exp_from = b_elements[4].text
#             else:
#                 exp_from = np.nan
#                 self.log.error('Не найдено значение для `exp_from`. Замена на np.nan.')
#
#
#             try:
#                 date_created = dateparser.parse(r2_soup.find('p', attrs={'class': 'text-left'}).text.strip(),
#                                                 languages=['ru']).date()
#             except Exception as e:
#                 self.log.error(f'Ошибка при обработке даты: {e}')
#                 date_created = datetime.now().date()
#             description = r2_soup.find('div', attrs={'class': "row p-y-3"}).text.strip()
#             description = '\n'.join(description[:description.find('Откликнуться на вакансию')].replace(
#                 'Контактная информация работодателя станет доступна сразу после того, как вы оставите свой отклик на эту вакансию.',
#                 '').split('\n')[1:])
#
#             self.df.loc[self.df['vacancy_id'] == url, 'date_created'] = date_created
#             self.df.loc[self.df['vacancy_id'] == url, 'description'] = description
#             self.df.loc[self.df['vacancy_id'] == url, 'exp_from'] = exp_from
#
#             time.sleep(2)
#
#     def find_vacancies(self):
#         self.log.info("Поиск вакансий...")
#         for prof in self.profs:
#             prof_name = prof['fullName']
#             self.log.info("Обработка профессии: " + prof_name)
#
#             try:
#                 search_url = self.url + f'?search%5Bquery%5D={prof_name.replace(" ", "+")}'
#                 self.browser.get(search_url)
#                 time.sleep(3)
#                 search = self.browser.find_element(By.XPATH, '//*[@id="search_query"]')
#                 search.send_keys(prof_name)
#                 search.send_keys(Keys.ENTER)
#                 time.sleep(3)
#
#                 if self.browser.find_element(By.CSS_SELECTOR, '.h2, h2').text == 'Вакансий не найдено':
#                     continue
#
#                 pagination_text = self.browser.find_element(By.CLASS_NAME, 'pagination').text.split('\n')
#                 if len(pagination_text) > 1:
#                     last_page = int(pagination_text[-2])
#                 else:
#                     last_page = 2
#                     self.log.error('Не найдено значение для `last_page`. Замена на 2.')
#
#             except Exception as e:
#                 self.log.error(f"Произошла ошибка {e}")
#
#             self.log.info('Страниц на обработку: {}'.format(last_page))
#
#             for i in range(1, last_page + 1):
#                 page_url = search_url + f'&page={i}'
#                 yield page_url

class RemoteJobParser(BaseJobParser):
    def main_page(self, url):
        self.log.info(f'Анализируется главная страница {url}')
        try:
            r = requests.get(url, headers=self.headers, timeout=10)
        except:
            self.log.error('не удалось получить доступ к странице')
            return

        soup = BeautifulSoup(r.content, 'html.parser')
        divs = soup.find_all('div', attrs={'class': 'col-xs-10 col-sm-10 col-md-10 col-lg-10 text-left'})
        for div in divs:
            salary_info = ' '.join(div.find('h3').text.split())

            if salary_info == "з.п. не указана":
                salary_from = None
                salary_to = None
            else:
                try:
                    salary_info = salary_info.replace(' ', '').replace('руб.', '')

                    if 'от' in salary_info:
                        salary_info = salary_info.split('от')[1]

                    salary_parts = salary_info.split('до')

                    if len(salary_parts) > 1:
                        salary_from = int(salary_parts[0])
                        salary_to = int(salary_parts[1])
                    else:
                        salary_values = salary_parts[0].split('от')
                        if len(salary_values) > 1:
                            salary_from = int(salary_values[1])
                            salary_to = np.nan
                        else:
                            salary_from = np.nan
                            salary_to = np.nan

                except Exception as e:
                    self.log.error(f"Ошибка при обработке информации о зарплате: {e}")
                    salary_from = np.nan
                    salary_to = np.nan

            vacancy_id = 'https://remote-job.ru/' + div.find('a').get('href')
            vacancy_name = ' '.join(div.find('a').text.split())
            company = div.find_all('small')[1].text.strip()

            self.url_l.append(vacancy_id)

            self.df['vacancy_id'] = vacancy_id
            self.df['vacancy_name'] = vacancy_name
            self.df['towns'] = None
            self.df['company'] = company
            self.df['salary_from'] = salary_from
            self.df['salary_to'] = salary_to
            self.df['description'] = None
            self.df['job_format'] = 'Удаленная работа'
            self.df['source_vac'] = 'https://remote-job.ru/'
            self.df['date_created'] = None
            self.df['date_of_download'] = datetime.now().date()
            self.df['status'] = 'existing'
            self.df['version_vac'] = 1
            self.df['actual'] = 1

    def url_page(self):
        for url in self.url_l:
            self.log.info(f'Изучается страница с URL {url}')
            try:
                r2 = requests.get(url, headers=self.headers, timeout=15)
            except:
                self.log.error('Не удалось получить доступ к вакансии:', url)
                self.df = self.df[self.df['vacancy_id'] != url]
                continue

            soup2 = BeautifulSoup(r2.content, 'html.parser')

            try:
                date_created = dateparser.parse(soup2.find('p', attrs={'class': 'text-left'}).text.strip(),
                                                languages=['ru']).date()
            except:
                date_created = datetime.now().date()

            text = soup2.find('div', attrs={'class': "row p-y-3"}).text.strip()
            description = '\n'.join(text[:text.find('Откликнуться на вакансию')].replace(
                'Контактная информация работодателя станет доступна сразу после того, как вы оставите свой отклик на эту вакансию.',
                '').split('\n')[1:]).strip()

            # Обновление записей в датафрейме
            index = self.df['vacancy_id'] == url
            self.df.loc[index, 'date_created'] = date_created
            self.df.loc[index, 'description'] = description

    def find_vacancies(self):
        options.add_argument('--headless')
        self.log = log
        self.df = pd.DataFrame(
            columns=['vacancy_id', 'vacancy_name', 'towns', 'company', 'salary_from', 'salary_to',
                     'description', 'job_format', 'source_vac', 'date_created',
                     'date_of_download', 'status', 'version_vac', 'actual'])
        ua = UserAgent().chrome
        self.headers = {'User-Agent': ua}
        self.url_l = []
        options.add_argument(f'--user-agent={ua}')

        self.log.info('Поиск вакансий начался...')

        for prof in self.profs:
            prof_name = prof['fullName']
            self.log.info(f'Поиск вакансий для профессии "{prof_name}"')
            self.browser.get(self.url)
            time.sleep(3)
            search = self.browser.find_element(By.XPATH, '//*[@id="search_query"]')
            search.send_keys(prof_name)
            search.send_keys(Keys.ENTER)

            if self.browser.find_element(By.CSS_SELECTOR, '.h2, h2').text == 'Вакансий не найдено':
                continue
            try:
                last_page = int(self.browser.find_element(By.CLASS_NAME, 'pagination').text.split('\n')[-2])
            except NoSuchElementException:
                last_page = 2

            vacancy_url = self.browser.current_url
            self.log.info(f'Страниц на обработку: {last_page}')

            for i in range(1, last_page + 1):
                self.main_page(vacancy_url + '&page={}'.format(i))
                self.url_page()
                # self.log.info('Обработана страница', i)

        self.df.drop_duplicates()
        self.log.info(
            "Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")

    def save_df(self):
        self.log.info("Сохранение результатов в Базу Данных...")

        self.cur = self.conn.cursor()

        self.df['salary_from'] = self.df['salary_from'].fillna(0).astype(np.int64)
        self.df['salary_to'] = self.df['salary_to'].fillna(0).astype(np.int64)

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        register_adapter(np.float64, addapt_numpy_float64)
        register_adapter(np.int64, addapt_numpy_int64)

        try:
            if not self.df.empty:
                self.df.drop_duplicates()
                print(self.df.iloc[0].to_string())
                print("| ".join(map(str, self.df.iloc[0])))
                self.log.info(
                    "Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
                table_name = 'raw_remote'
                data = [tuple(x) for x in self.df.to_records(index=False)]
                query = f"INSERT INTO {table_name} (vacancy_id, vacancy_name, towns, company, salary_from, " \
                        f"salary_to, description, job_format, source_vac, date_created, date_of_download, " \
                        f"status, version_vac, actual) VALUES %s"
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

# class RemoteJobParser(BaseJobParser):
#     """
#     Парсер вакансий для удалённой работы, наследованный от BaseJobParser
#     """
#     def find_vacancies(self):
#         """
#         Метод для нахождения вакансий для удалённой работы
#         """
#         self.cur = self.conn.cursor()
#         self.log.info("Создан DataFrame для записи вакансий")
#         self.df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'towns', 'company', 'salary_to', 'exp_from',
#                                         'description', 'job_format', 'source_vac', 'date_created', 'date_of_download',
#                                         'status', 'version_vac', 'actual'])
#
#         self.log.info("Создан DataFrame для записи вакансий")
#         self.browser.implicitly_wait(3)
#
#         self.main_page(self.url)
#         self.url_page()
#
#         div = self.browser.find_elements(By.CSS_SELECTOR, '.col-xs-10')
#         for i in div:
#             vac_info = {}
#             vac_info['vacancy_name'] = i.find_element(By.CSS_SELECTOR, '.navbar li, a, button').text
#             vac_info['company'] = i.find_elements(By.TAG_NAME, 'small')[1].text.strip()
#             vac_info['salary_to'] = i.find_element(By.TAG_NAME, 'h3').text
#             vac_info['vacancy_id'] = i.find_element(By.TAG_NAME, 'a').get_attribute('href')
#
#             self.df.loc[len(self.df)] = vac_info
#
#         self.df = self.df.drop_duplicates()
#         self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")
#         self.df['date_of_download'] = datetime.now().date()
#         self.df['source_vac'] = url_remote
#         self.df['status'] = 'existing'
#         self.df['version_vac'] = 1
#         self.df['actual'] = 1
#
#     def url_page(self):
#         for url in self.df['vacancy_id']:
#             self.browser.get(url)
#             time.sleep(2)
#             try:
#                 self.df.loc[self.df['vacancy_id'] == url, 'exp_from'] = \
#                 self.browser.find_elements(By.CSS_SELECTOR, 'b, strong')[4].text
#                 self.df.loc[self.df['vacancy_id'] == url, 'date_created'] = self.browser.find_element(By.XPATH,
#                                                                                                    "//*[@class='text-left']").text
#
#                 text_tag = self.browser.find_element(By.XPATH, '//*[@class="row p-y-3"]')
#                 text = "\n".join((text_tag.text.strip().split("\n")[2:])).replace('\r', '')
#                 description = text[:text.find('Откликнуться на вакансию')].strip().replace(
#                     'Контактная информация работодателя станет доступна сразу после того, как вы оставите свой отклик на эту вакансию.',
#                     '')
#                 self.df.loc[self.df['vacancy_id'] == url, 'description'] = description
#
#                 self.df.loc[self.df['vacancy_id'] == url, 'towns'] = '-'
#                 self.df.loc[self.df['vacancy_id'] == url, 'job_format'] = 'удаленная работа'
#
#             except Exception as e:
#                 self.log.error(f"Произошла ошибка: {e}, ссылка {url}")
#                 self.df = self.df[self.df['vacancy_id'] != url]
#                 continue
#
#             time.sleep(1)
#
#     def save_df(self):
#         """
#         Метод для сохранения данных из pandas DataFrame
#         """
#         # Ваш код здесь для сохранения данных
#         # Та же логика, что и в методе save_df класса VKJobParser


db_manager = DatabaseManager(conn=conn)

# def init_run_vk_parser(**context):
#     """
#     Основной вид задачи для запуска парсера для вакансий VK
#     """
#     log = context['ti'].log
#     log.info('Запуск парсера ВК')
#     try:
#         parser = VKJobParser(url_vk, profs, log, conn)
#         parser.find_vacancies()
#         parser.find_vacancies_description()
#         parser.save_df()
#         parser.stop()
#         log.info('Парсер ВК успешно провел работу')
#     except Exception as e:
#         log.error(f'Ошибка во время работы парсера ВК: {e}')

# def init_run_remote_parser(**context):
#     """
#     Задача для запуска парсера вакансий удалённой работы
#     """
#     log = context['ti'].log
#     log.info('Запуск парсера удалённой работы')
#     try:
#         # Передайте полный URL для вашего сайта с вакансиями непосредственно коду:
#         parser = RemoteJobParser('https://remote-job.ru/search?search%5Bquery%5D=&search%5BsearchType%5D=vacancy',
#                                  profs, log, conn)
#
#         # В этом цикле мы получаем все URL страниц и обрабатываем их
#         for page_url in parser.find_vacancies():
#             parser.retrieve_page_data(page_url)
#             parser.find_vacancies_description()
#             parser.save_df()
#             log.info('Обработана страница', )
#
#         parser.stop()
#         log.info('Парсер удалённой работы успешно провёл работу')
#     except Exception as e:
#         log.error(f'Ошибка во время работы парсера удалённой работы: {e}')

def init_run_remote_job_parser(**context):
    log = context['ti'].log
    log.info('Запуск парсера remote-job')
    try:
        parser = RemoteJobParser('https://remote-job.ru/search?search%5Bquery%5D=&search%5BsearchType%5D=vacancy', profs, log, conn)
        parser.find_vacancies()
        parser.save_df()
        parser.stop()
        log.info('Парсер remote-job успешно провел работу')
    except Exception as e:
        log.error(f'Ошибка во время работы парсера remote-job: {e}')


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

# parse_vkjobs = PythonOperator(
#     task_id='parse_vkjobs',
#     python_callable=init_run_vk_parser,
#     provide_context=True,
#     dag=initial_dag
# )

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
