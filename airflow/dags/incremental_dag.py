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
                schedule_interval='0 */6 * * *',
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

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_button.clear()

        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")

    def find_values_in_db(self):
        """
        Метод поиска измененных вакансий VKJobParser до парсинга описаний.
        """
        try:
            if not self.df.empty:
                self.cur = self.conn.cursor()
                self.log.info('Поиск измененных вакансий VKJobParser до парсинга описаний')
                table_name = raw_tables[0]
                query = f"SELECT vacancy_id, vacancy_name, towns, company " \
                        f"FROM {table_name} " \ 
                        f"WHERE version_vac = (SELECT max(version_vac) FROM {table_name}) " \ 
                        f"ORDER BY date_of_download DESC, version_vac DESC " \
                        f"LIMIT 1"
                self.cur.execute(query)
                rows_in_db = self.cur.fetchall()  # получаем все строки из БД

                # Создаем список для хранения индексов строк, которые нужно удалить из self.df
                rows_to_delete = []

                if rows_in_db:
                    for row_db in rows_in_db:
                        for index, row_df in self.df.iterrows():
                            if row_db[0] == row_df['vacancy_id']:
                                if row_db[1] == row_df['vacancy_name'] and \
                                        row_db[2] == row_df['towns'] and  \
                                        row_db[3] == row_df['company']:
                                    rows_to_delete.append(index)

                self.df = self.df.drop(rows_to_delete)  # удаляем строки из self.df
                self.log.info(f'Количество вакансий для парсинга описаний вакансий: {len(self.df)}.')
                self.cur.close()
        except Exception as e:
            self.log.error(f"Произошла ошибка: {e}")

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
        self.cur = self.conn.cursor()
        try:
            if not self.df.empty:
                self.df['date_created'] = datetime.now().date()
                self.df['date_of_download'] = datetime.now().date()
                self.df['source_vac'] = url_vk
                self.df['status'] = 'existing'
                self.df['version_vac'] = 1
                self.df['actual'] = 1

                table_name = raw_tables[0]
                self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
                log.info('Собираем вакансии для сравнения')
                query = f"SELECT vacancy_id FROM {table_name} " \
                        f"WHERE version_vac = (SELECT max(version_vac) FROM {table_name}) " \
                        f"ORDER BY date_of_download DESC, version_vac DESC LIMIT 1"

                self.cur.execute(query)
                links_in_db = self.cur.fetchall()
                links_in_db_set = set(vacancy_id for vacancy_id, in links_in_db)
                links_in_parsed = set(self.df['vacancy_id'])

                links_to_close = links_in_db_set - links_in_parsed

                dataframe_to_closed = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'towns', 'company', 'description', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'date_closed', 'version_vac', 'actual'
                ])

                dataframe_to_update = pd.DataFrame(columns=[
                    'vacancy_id', 'vacancy_name', 'towns', 'company', 'description', 'source_vac',
                    'date_created', 'date_of_download', 'status', 'version_vac', 'actual'
                ])

                log.info('Создаем датафрейм dataframe_to_closed')
                if links_to_close:
                    for link in links_to_close:
                        records_to_close = self.cur.execute(
                            f"""
                            SELECT vacancy_id, vacancy_name, towns, company, description, source_vac,
                            date_created, date_of_download, status, version_vac, actual
                            FROM {table_name}
                            WHERE vacancy_id = '{link}'
                            AND status != 'closed'
                            AND actual != '-1'
                            AND version_vac = (
                                SELECT max(version_vac) FROM {table_name}
                                WHERE vacancy_id = '{link}'
                            )
                            ORDER BY date_of_download DESC, version_vac DESC LIMIT 1
                            """
                        )
                        if records_to_close:
                            for record in records_to_close:
                                data = {
                                    'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                    'company': record[3], 'description': record[4], 'source_vac': record[5],
                                    'date_created': record[6], 'date_of_download': datetime.now().date(),
                                    'status': 'closed', 'date_closed': datetime.now().date(),
                                    'version_vac': record[-2] + 1, 'sign': -1
                                }
                                dataframe_to_closed = pd.concat([dataframe_to_closed, pd.DataFrame(data, index=[0])])
                                log.info('Датафрейм dataframe_to_closed создан')
                else:
                    log.info('Список links_to_close пуст')

                log.info('Присваиваем статусы изменений')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    link = record[0]
                    records_in_db = self.cur.execute(
                        f"""SELECT vacancy_id, vacancy_name, towns, company, description, source_vac, 
                            date_created, date_of_download, status, version_vac, actual 
                            FROM {table_name} 
                            WHERE vacancy_id = '{link}' 
                            ORDER BY date_of_download DESC, version_vac DESC 
                            LIMIT 1
                        """)
                    if records_in_db:
                        for old_record in records_in_db:
                            old_status = old_record[-3]
                            next_version = old_record[-2] + 1
                            if old_status == 'new':
                                data_new_vac = {'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                        'company': record[3], 'description': record[4],
                                        'source_vac': record[5], 'date_created': old_record[6],
                                        'date_of_download': datetime.now().date(), 'status': 'existing',
                                        'version_vac': next_version, 'actual': 1}
                                dataframe_to_update = pd.concat([dataframe_to_update,
                                                                 pd.DataFrame(data_new_vac, index=[0])])
                            if old_status == 'existing':
                                if old_record[1] == record[1] and old_record[2] == record[2] and old_record[3] == \
                                        record[3] and old_record[4] == record[4] and old_record[5] == record[5] and \
                                        old_record[6] == record[6]:
                                    pass
                                else:
                                    dataframe_to_update = pd.concat(
                                        [dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])])

                            if old_status == 'closed':
                                if link in links_in_parsed:
                                    data_clos_new = {'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                            'company': record[3], 'description': record[4],
                                            'source_vac': record[5], 'date_created': record[6],
                                            'date_of_download': datetime.now().date(), 'status': 'new',
                                            'version_vac': next_version, 'actual': 1}
                                    dataframe_to_update = pd.concat(
                                        [dataframe_to_update, pd.DataFrame(data_clos_new, index=[0])])
                    else:
                        data_full_new = {'vacancy_id': link, 'vacancy_name': record[1], 'towns': record[2],
                                         'company': record[3], 'description': record[4],
                                         'source_vac': record[5], 'date_created': record[6],
                                         'date_of_download': datetime.now().date(), 'status': 'new', 'version_vac': 1,
                                         'actual': 1}
                        dataframe_to_update = pd.concat([dataframe_to_update, pd.DataFrame(data_full_new, index=[0])])


                    if not dataframe_to_closed.empty:
                        try:
                            data_tuples_to_insert = [tuple(x) for x in dataframe_to_closed.to_records(index=False)]
                            cols = ",".join(dataframe_to_closed.columns)
                            log.info(f'Добавляем строки удаленных вакансий в таблицу {table_name}.')
                            self.cur.execute(
                                f"""INSERT INTO {table_name} ({cols}) VALUES""", data_tuples_to_insert)
                            self.conn.commit()
                            self.log.info(f"Количество отмеченных удаленными вакансий VK: "
                                          f"{str(len(dataframe_to_closed))}, обновлена таблица {table_name}.")
                        except Exception as e:
                            self.log.error(f"Ошибка при вставке записей из dataframe_to_closed: {e}")
                            self.log.error(f"Содержимое dataframe_to_closed: {dataframe_to_closed}")
                            raise

                    else:
                        self.log.info(
                            f"Удаленных вакансий VK нет, таблица {table_name} в БД {config['database']} "
                            f"не изменена.")

                    if not dataframe_to_update.empty:
                        try:
                            data_tuples_to_insert = [tuple(x) for x in dataframe_to_update.to_records(index=False)]
                            cols = ",".join(dataframe_to_update.columns)
                            log.info(f'Обновляем таблицу {table_name}.')
                            self.cur.execute(
                                f"""INSERT INTO {table_name} ({cols}) VALUES""", data_tuples_to_insert)
                            self.conn.commit()
                            self.log.info(f"Количество измененных вакансий VK: "
                                          f"{str(len(dataframe_to_update))}, обновлена таблица {table_name}.")
                        except Exception as e:
                            self.log.error(f"Ошибка при вставке записей из dataframe_to_update: {e}")
                            self.log.error(f"Содержимое dataframe_to_update: {dataframe_to_update.columns}")
                            raise

                    else:
                        self.log.info(
                            f"Измененных вакансий VK нет, таблица {table_name} в БД {config['database']} "
                            f"не изменена.")

                        # # совершение транзакции
                        # self.conn.commit()
                        # self.log.info("Транзакция успешно завершена")

                    # except Exception as e:
                    #     self.log.error(f"Ошибка в транзакции. Отмена всех остальных операций транзакции, {e}")
                    #     self.conn.rollback()
                    # finally:
                    #     if self.conn:
                    #         self.cur.close()
                    #         self.conn.close()
                    #         self.log.info("Соединение с PostgreSQL закрыто")
                # Код для вставки новых записей в таблицу core_fact_table

        #         dataframe_to_upd_core = dataframe_to_update[['link', 'name', 'location', 'company', 'salary',
        #          'description', 'date_created', 'date_of_download', 'status']]
        #
        #         if not dataframe_to_upd_core.empty:
        #             try:
        #                 log.info(f'Обновляем таблицу core_fact_table.')
        #                 core_fact_data_tuples = [tuple(x) for x in dataframe_to_upd_core.to_records(index=False)]
        #                 cols = ",".join(dataframe_to_upd_core.columns)
        #                 self.client.execute(f"INSERT INTO {config['database']}.core_fact_table ({cols}) VALUES",
        #                                     core_fact_data_tuples)
        #                 self.log.info(f"Количество строк вставлено в core_fact_table: "
        #                               f"{len(core_fact_data_tuples)}, обновлена таблица core_fact_table "
        #                               f"в БД {config['database']}.")
        #
        #             except Exception as e:
        #                 self.log.error(f"Ошибка при вставке записей из dataframe_to_upd_core: {e}")
        #                 self.log.error(f"Содержимое dataframe_to_upd_core: {dataframe_to_upd_core.columns}")
        #                 raise
        #
        #         else:
        #             self.log.info(
        #                 f"Обновленных вакансий VK нет, таблица 'core_fact_table' в БД {config['database']} не изменена.")
        #
        #         dataframe_to_delete = dataframe_to_closed[['link', 'name', 'location', 'level', 'company', 'salary',
        #          'description', 'date_created', 'date_of_download', 'status']]
        #         # Код для удаления "closed" записей в таблице core_fact_table
        #         if not dataframe_to_delete.empty:
        #             try:
        #                 log.info(f'Удаляем из core_fact_table закрытые вакансии.')
        #                 dataframe_to_delete_tuples = [tuple(x) for x in
        #                                               dataframe_to_delete[['link']].to_records(index=False)]
        #                 for to_delete in dataframe_to_delete_tuples:
        #                     self.client.execute(
        #                         f"DELETE FROM {config['database']}.core_fact_table WHERE link = '{to_delete[0]}'")
        #                 self.log.info(f"Количество строк удалено из core_fact_table: "
        #                               f"{len(dataframe_to_delete_tuples)}, обновлена таблица core_fact_table в БД "
        #                               f"{config['database']}.")
        #
        #             except Exception as e:
        #                 self.log.error(f"Ошибка при вставке записей из dataframe_to_delete: {e}")
        #                 self.log.error(f"Содержимое dataframe_to_upd_core: {dataframe_to_delete.columns}")
        #                 raise
        #
        #         else:
        #             self.log.info(f"Таблица 'core_fact_table' не изменена, нет строк для удаления.")
        #
        #     else:
        #         self.log.info(f"Не найдено вакансий при парсинге данных.")
        #
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
        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'level', 'company', 'salary', 'description',
                                        'date_created', 'date_of_download', 'status', 'date_closed', 'version', 'sign'])
        self.log.info('Старт парсинга вакансий')
        self.browser.implicitly_wait(1)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs:
            input_str = self.browser.find_element(By.XPATH,
                                                  '/html/body/div/div/div[2]/div[3]/div/div/div[2]/div/div/div/div/input')

            input_str.send_keys(f"{prof['fullName']}")
            click_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div/div[2]/div[3]/div/div/div[2]/div/div/div/div/button')
            click_button.click()
            time.sleep(5)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                # Подсчет количества предложений
                vacs_bar = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div/div[2]/div[3]/div/div/div[3]/div/div[3]/div[2]')
                vacs = vacs_bar.find_elements(By.TAG_NAME, 'div')

                vacs = [div for div in vacs if 'styled__Card-sc-192d1yv-1' in str(div.get_attribute('class'))]
                self.log.info(f"Парсим вакансии по запросу: {prof['fullName']}")
                self.log.info(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    data = vac.find_elements(By.CLASS_NAME, 'Text-sc-36c35j-0')
                    vac_info['name'] = data[0].text
                    vac_info['location'] = data[2].text
                    vac_info['company'] = data[3].text
                    vac_info['date_created'] = data[4].text
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")
                input_str.clear()

        # Удаление дубликатов в DataFrame
        self.df = self.df.drop_duplicates()

        self.df['date_created'] = self.df['date_created'].apply(lambda x: dateparser.parse(x, languages=['ru']))
        self.df['date_created'] = pd.to_datetime(self.df['date_created']).dt.to_pydatetime()

        self.df['date_of_download'] = datetime.now().date()
        self.df['level'] = None
        self.df['salary'] = np.nan
        # self.df['version'] = 1
        # self.df['sign'] = 1

        log.info(f'Всего вакансий после удаления дубликатов: {len(self.df)}.')

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для SberJobParser.
        """
        if not self.df.empty:
            self.log.info(f"Старт парсинга описаний вакансий")
            self.df['description'] = None
            for descr in self.df.index:
                try:
                    link = self.df.loc[descr, 'link']
                    self.browser.get(link)
                    self.browser.delete_all_cookies()
                    time.sleep(3)
                    desc = self.browser.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[3]'
                                                               '/div/div/div[3]/div[3]').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'link']}")
                    pass
        else:
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Sber
        """
        self.df['date_created'] = self.df['date_created'].dt.date

        table_name = 'raw_sber'

        try:
            self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
            log.info('Собираем вакансии для сравнения')
            links_in_db = self.client.execute(f"SELECT link FROM {config['database']}.{table_name}")

            links_in_db_set = set(link for link, in links_in_db)
            links_in_parsed = set(self.df['link'])

            if not self.df.empty:
                links_to_close = links_in_db_set - links_in_parsed

                dataframe_to_closed = pd.DataFrame(columns=[
                    'link', 'name', 'location', 'level', 'company', 'salary', 'description',
                    'date_created', 'date_of_download', 'status', 'date_closed', 'version', 'sign'
                ])

                dataframe_to_update = pd.DataFrame(columns=[
                    'link', 'name', 'location', 'level', 'company', 'salary', 'description',
                    'date_created', 'date_of_download', 'status', 'version', 'sign'
                ])

                log.info('Присваиваем статус "closed"')
                for link in links_to_close:
                    records_to_close = self.client.execute(
                        f"""
                                    SELECT * FROM {config['database']}.{table_name}
                                    WHERE link = '{link}' 
                                    AND status != 'closed' 
                                    AND sign != '-1' 
                                    AND version = (
                                        SELECT max(version) FROM {config['database']}.{table_name}
                                        WHERE link = '{link}')
                                    ORDER BY date_of_download DESC, version DESC LIMIT 1
                                    """
                    )
                    for record in records_to_close:
                        data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                'company': record[4], 'salary': record[5], 'description': record[6],
                                'date_created': record[7],
                                'date_of_download': datetime.now().date(), 'status': 'closed',
                                'date_closed': datetime.now().date(), 'version': record[11] + 1, 'sign': -1}
                        dataframe_to_closed = pd.concat([dataframe_to_closed, pd.DataFrame(data, index=[0])])

                log.info('Присваиваем статусы изменений')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    # self.log.info(f"Проверка типов данных в tuple: \n {[type(r) for r in record]}")
                    link = record[0]
                    records_in_db = self.client.execute(
                        f"SELECT * FROM {config['database']}.{table_name} WHERE link = '{link}' ORDER BY date_of_download "
                        f"DESC, version DESC LIMIT 1")
                    if records_in_db:
                        for old_record in records_in_db:
                            old_status = old_record[-3]
                            next_version = old_record[11] + 1
                            if old_status == 'new':
                                data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                        'company': record[4], 'salary': record[5], 'description': record[6],
                                        'date_created': record[7],
                                        'date_of_download': datetime.now().date(), 'status': 'existing',
                                        'version': next_version, 'sign': 1}
                                dataframe_to_update = pd.concat([dataframe_to_update, pd.DataFrame(data, index=[0])])
                            if old_status == 'existing':
                                if old_record[1] == record[1] and old_record[2] == record[2] and old_record[3] == \
                                        record[3] and old_record[4] == record[4] and old_record[5] == record[5] and \
                                        old_record[6] == record[6]:
                                    pass
                                else:
                                    data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                            'company': record[4], 'salary': record[5], 'description': record[6],
                                            'date_created': old_record[7],
                                            'date_of_download': datetime.now().date(), 'status': 'existing',
                                            'version': next_version, 'sign': 1}
                                    dataframe_to_update = pd.concat(
                                        [dataframe_to_update, pd.DataFrame(data, index=[0])])

                            if old_status == 'closed':
                                if link in links_in_parsed:
                                    data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                            'company': record[4], 'salary': record[5], 'description': record[6],
                                            'date_created': record[7],
                                            'date_of_download': datetime.now().date(), 'status': 'new',
                                            'version': next_version,
                                            'sign': 1}
                                    dataframe_to_update = pd.concat(
                                        [dataframe_to_update, pd.DataFrame(data, index=[0])])
                    else:
                        data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                'company': record[4], 'salary': record[5], 'description': record[6],
                                'date_created': record[7], 'date_of_download': datetime.now().date(),
                                'status': 'new', 'version': 1, 'sign': 1}
                        dataframe_to_update = pd.concat([dataframe_to_update, pd.DataFrame(data, index=[0])])

                if not dataframe_to_closed.empty:
                    try:
                        data_tuples_to_insert = [tuple(x) for x in dataframe_to_closed.to_records(index=False)]
                        cols = ",".join(dataframe_to_closed.columns)
                        log.info(f'Добавляем строки удаленных вакансий в таблицу {table_name}.')
                        self.client.execute(f"INSERT INTO {config['database']}.{table_name} ({cols}) VALUES",
                                            data_tuples_to_insert)
                        self.log.info(f"Количество отмеченных удаленными вакансий Sber: "
                                      f"{str(len(dataframe_to_closed))}, обновлена таблица {table_name} в БД "
                                      f"{config['database']}.")
                    except Exception as e:
                        self.log.error(f"Ошибка при вставке записей из dataframe_to_closed: {e}")
                        self.log.error(f"Содержимое dataframe_to_closed: {dataframe_to_closed}")
                        raise

                else:
                    self.log.info(
                        f"Нет вакансий Sber для удаления, таблица {table_name} в БД {config['database']} не изменена.")

                if not dataframe_to_update.empty:
                    try:
                        # dataframe_to_update['date_created'] = dataframe_to_update['date_created'].strftime('%Y-%m-%d')
                        data_tuples_to_insert = [tuple(x) for x in dataframe_to_update.to_records(index=False)]
                        cols = ",".join(dataframe_to_update.columns)
                        log.info(f'Вносим изменения в таблицу {table_name}.')
                        self.client.execute(f"INSERT INTO {config['database']}.{table_name} ({cols}) VALUES",
                                            data_tuples_to_insert)
                        self.log.info(f"Количество измененных вакансий Sber: "
                                      f"{str(len(dataframe_to_update))}, обновлена таблица {table_name} в БД "
                                      f"{config['database']}.")
                    except Exception as e:
                        self.log.error(f"Ошибка при вставке записей из dataframe_to_update: {e}")
                        self.log.error(f"Содержимое dataframe_to_update: {dataframe_to_update.columns}")
                        raise
                else:
                    self.log.info(
                        f"Измененных вакансий Sber нет, таблица {table_name} в БД {config['database']} не изменена.")

                # Код для вставки новых записей в таблицу core_fact_table

                dataframe_to_upd_core = dataframe_to_update[['link', 'name', 'location', 'level', 'company', 'salary',
                                                             'description', 'date_created', 'date_of_download',
                                                             'status']]

                if not dataframe_to_upd_core.empty:
                    try:
                        log.info(f'Обновляем таблицу core_fact_table.')
                        core_fact_data_tuples = [tuple(x) for x in dataframe_to_upd_core.to_records(index=False)]
                        cols = ",".join(dataframe_to_upd_core.columns)
                        self.client.execute(f"INSERT INTO {config['database']}.core_fact_table ({cols}) VALUES",
                                            core_fact_data_tuples)
                        self.log.info(f"Количество строк вставлено в core_fact_table: "
                                      f"{len(core_fact_data_tuples)}, обновлена таблица core_fact_table "
                                      f"в БД {config['database']}.")

                    except Exception as e:
                        self.log.error(f"Ошибка при вставке записей из dataframe_to_upd_core: {e}")
                        self.log.error(f"Содержимое dataframe_to_upd_core: {dataframe_to_upd_core.columns}")
                        raise

                else:
                    self.log.info(
                        f"Обновленных вакансий Sber нет, таблица 'core_fact_table' в БД {config['database']} не изменена.")

                dataframe_to_delete = dataframe_to_closed[['link', 'name', 'location', 'level', 'company', 'salary',
                                                           'description', 'date_created', 'date_of_download', 'status']]

                # Код для удаления "closed" записей в таблице core_fact_table
                if not dataframe_to_delete.empty:
                    try:
                        log.info(f'Удаляем из core_fact_table закрытые вакансии.')
                        dataframe_to_delete_tuples = [tuple(x) for x in
                                                      dataframe_to_delete[['link']].to_records(index=False)]
                        for to_delete in dataframe_to_delete_tuples:
                            self.client.execute(
                                f"DELETE FROM {config['database']}.core_fact_table WHERE link = '{to_delete[0]}'")
                        self.log.info(f"Количество строк удалено из core_fact_table: "
                                      f"{len(dataframe_to_delete_tuples)}, обновлена таблица core_fact_table в БД "
                                      f"{config['database']}.")

                    except Exception as e:
                        self.log.error(f"Ошибка при вставке записей из dataframe_to_delete: {e}")
                        self.log.error(f"Содержимое dataframe_to_upd_core: {dataframe_to_delete.columns}")
                        raise

                else:
                    self.log.info(f"Таблица 'core_fact_table' не изменена, нет строк для удаления.")

            else:
                self.log.info(f"Не найдено вакансий при парсинге данных.")

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise


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
        self.df = pd.DataFrame(columns=['link', 'name', 'location', 'level', 'company', 'date_created',
                                        'date_of_download', 'status', 'version', 'sign', 'description'])

        self.log.info("Создан DataFrame для записи вакансий")

        self.browser.implicitly_wait(3)
        vacs = self.browser.find_elements(By.TAG_NAME, 'tr')
        vacs = [div for div in vacs if 'eM3bvP' in str(div.get_attribute('class'))]
        for vac in vacs:
            try:
                vac_info = {}
                vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                data = vac.find_elements(By.CLASS_NAME, 'gM3bvP')
                vac_info['name'] = data[0].text
                vac_info['level'] = data[1].text
                vac_info['location'] = data[2].text
                self.df.loc[len(self.df)] = vac_info
            except Exception as e:
                self.log.error(f"Произошла ошибка: {e}")

            self.df = self.df.drop_duplicates()
            self.df['company'] = 'Тинькофф'
            self.df['date_created'] = datetime.now().date()
            self.df['date_of_download'] = datetime.now().date()
            self.df['description'] = None
            self.df['status'] = 'existing'
            self.df['sign'] = 1
            self.df['version'] = 1

            self.log.info(
                f"Парсер завершил работу. Обработано {len(vacs)} вакансий. Оставлены только уникальные записи. "
                f"Записи обновлены данными о компании, дате создания и загрузки.")

    # def all_vacs_parser(self):
    #     """
    #     Метод для нахождения вакансий с Tinkoff
    #     """
    #     self.df = pd.DataFrame(
    #         columns=['link', 'name', 'location', 'level', 'company', 'date_created', 'date_of_download', 'status',
    #                  'version', 'sign', 'description'])
    #
    #     self.log.info("Создан DataFrame для записи вакансий")
    #
    #     self.browser.implicitly_wait(3)
    #     try:
    #         vac_index = 0
    #         while True:
    #             try:
    #                 vac = self.browser.find_elements(By.CLASS_NAME, 'eM3bvP')[vac_index]
    #             except IndexError:
    #                 break  # Закончили обработку всех элементов
    #
    #             self.log.info(f"Обработка вакансии номер {vac_index + 1}")
    #
    #             vac_info = {}
    #             vac_info['link'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
    #             data = vac.find_elements(By.CLASS_NAME, 'gM3bvP')
    #             vac_info['name'] = data[0].text
    #             vac_info['level'] = data[1].text
    #             vac_info['location'] = data[2].text
    #             self.df.loc[len(self.df)] = vac_info
    #             vac_index += 1
    #
    #         self.df = self.df.drop_duplicates()
    #         self.df['company'] = 'Тинькофф'
    #         self.df['date_created'] = datetime.now().date()
    #         self.df['date_of_download'] = datetime.now().date()
    #         self.df['description'] = None
    #         self.df['status'] = 'existing'
    #         self.df['sign'] = 1
    #         self.df['version'] = 1
    #         self.df['salary'] = np.nan
    #
    #         self.log.info(
    #             f"Парсер завершил работу. Обработано {vac_index} вакансий. Оставлены только уникальные записи. "
    #             f"Записи обновлены данными о компании, дате создания и загрузки.")
    #
    #     except Exception as e:
    #         self.log.error(f"Произошла ошибка: {e}")

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
            self.log.info(f"Нет вакансий для парсинга")

    def save_df(self):
        """
        Метод для сохранения данных в базу данных Tinkoff
        """
        self.df['description'] = self.df['description'].astype(str)
        table_name = 'raw_tin'
        self.log.info("Общее количество вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")
        try:
            self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
            log.info('Собираем вакансии для сравнения')
            links_in_db = self.client.execute(f"SELECT link FROM {config['database']}.{table_name}")

            links_in_db_set = set(link for link, in links_in_db)
            links_in_parsed = set(self.df['link'])

            if not self.df.empty:
                links_to_close = links_in_db_set - links_in_parsed

                dataframe_to_closed = pd.DataFrame(columns=[
                    'link', 'name', 'location', 'level', 'company', 'salary', 'description',
                    'date_created', 'date_of_download', 'status', 'date_closed', 'version', 'sign'
                ])

                dataframe_to_update = pd.DataFrame(columns=[
                    'link', 'name', 'location', 'level', 'company', 'salary', 'description',
                    'date_created', 'date_of_download', 'status', 'version', 'sign'
                ])

                log.info('Присваиваем статус "closed"')
                for link in links_to_close:
                    records_to_close = self.client.execute(
                        f"""
                                    SELECT * FROM {config['database']}.{table_name}
                                    WHERE link = '{link}' 
                                    AND status != 'closed' 
                                    AND sign != '-1' 
                                    AND version = (
                                        SELECT max(version) FROM {config['database']}.{table_name}
                                        WHERE link = '{link}')
                                    ORDER BY date_of_download DESC, version DESC LIMIT 1
                                    """
                    )
                    for record in records_to_close:
                        data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                'company': record[4], 'salary': record[5], 'description': record[6],
                                'date_created': record[7],
                                'date_of_download': datetime.now().date(), 'status': 'closed',
                                'date_closed': datetime.now().date(), 'version': record[11] + 1, 'sign': -1}
                        dataframe_to_closed = pd.concat([dataframe_to_closed, pd.DataFrame(data, index=[0])])

                log.info('Присваиваем статусы изменений')
                data = [tuple(x) for x in self.df.to_records(index=False)]
                for record in data:
                    # self.log.info(f"Проверка типов данных в tuple: \n {[type(r) for r in record]}")
                    link = record[0]
                    records_in_db = self.client.execute(
                        f"SELECT * FROM {config['database']}.{table_name} WHERE link = '{link}' ORDER BY date_of_download "
                        f"DESC, version DESC LIMIT 1")
                    if records_in_db:
                        for old_record in records_in_db:
                            old_status = old_record[-3]
                            next_version = old_record[11] + 1
                            if old_status == 'new':
                                data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                        'company': record[4], 'salary': record[5], 'description': record[6],
                                        'date_created': old_record[7],
                                        'date_of_download': datetime.now().date(), 'status': 'existing',
                                        'version': next_version, 'sign': 1}
                                dataframe_to_update = pd.concat([dataframe_to_update, pd.DataFrame(data, index=[0])])
                            if old_status == 'existing':
                                if old_record[1] == record[1] and old_record[2] == record[2] and old_record[3] == \
                                        record[3] and old_record[4] == record[4] and old_record[5] == record[5] and \
                                        old_record[6] == record[6]:
                                    pass
                                else:
                                    data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                            'company': record[4], 'salary': record[5], 'description': record[6],
                                            'date_created': old_record[7],
                                            'date_of_download': datetime.now().date(), 'status': 'existing',
                                            'version': next_version, 'sign': 1}
                                    dataframe_to_update = pd.concat(
                                        [dataframe_to_update, pd.DataFrame(data, index=[0])])

                            if old_status == 'closed':
                                if link in links_in_parsed:
                                    data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                            'company': record[4], 'salary': record[5], 'description': record[6],
                                            'date_created': old_record[7],
                                            'date_of_download': datetime.now().date(), 'status': 'new',
                                            'version': next_version,
                                            'sign': 1}
                                    dataframe_to_update = pd.concat(
                                        [dataframe_to_update, pd.DataFrame(data, index=[0])])
                    else:
                        data = {'link': link, 'name': record[1], 'location': record[2], 'level': record[3],
                                'company': record[4], 'salary': record[5], 'description': record[6],
                                'date_created': datetime.now().date(), 'date_of_download': datetime.now().date(),
                                'status': 'new', 'version': 1, 'sign': 1}
                        dataframe_to_update = pd.concat([dataframe_to_update, pd.DataFrame(data, index=[0])])

                if not dataframe_to_closed.empty:
                    try:
                        data_tuples_to_insert = [tuple(x) for x in dataframe_to_closed.to_records(index=False)]
                        cols = ",".join(dataframe_to_closed.columns)
                        log.info(f'Добавляем строки удаленных вакансий в таблицу {table_name}.')
                        self.client.execute(f"INSERT INTO {config['database']}.{table_name} ({cols}) VALUES",
                                            data_tuples_to_insert)
                        self.log.info(f"Количество отмеченных удаленными вакансий VK: "
                                      f"{str(len(dataframe_to_closed))}, обновлена таблица {table_name} в БД "
                                      f"{config['database']}.")
                    except Exception as e:
                        self.log.error(f"Ошибка при вставке записей из dataframe_to_closed: {e}")
                        self.log.error(f"Содержимое dataframe_to_closed: {dataframe_to_closed}")
                        raise

                else:
                    self.log.info(
                        f"Удаленных вакансий VK нет, таблица {table_name} в БД {config['database']} не изменена.")

                if not dataframe_to_update.empty:
                    try:
                        data_tuples_to_insert = [tuple(x) for x in dataframe_to_update.to_records(index=False)]
                        cols = ",".join(dataframe_to_update.columns)
                        log.info(f'Обновляем таблицу {table_name}.')
                        self.client.execute(f"INSERT INTO {config['database']}.{table_name} ({cols}) VALUES",
                                            data_tuples_to_insert)
                        self.log.info(f"Количество измененных вакансий VK: "
                                      f"{str(len(dataframe_to_update))}, обновлена таблица {table_name} в БД "
                                      f"{config['database']}.")
                    except Exception as e:
                        self.log.error(f"Ошибка при вставке записей из dataframe_to_update: {e}")
                        self.log.error(f"Содержимое dataframe_to_update: {dataframe_to_update.columns}")
                        raise
                else:
                    self.log.info(
                        f"Измененных вакансий VK нет, таблица {table_name} в БД {config['database']} не изменена.")

                # Код для вставки новых записей в таблицу core_fact_table

                dataframe_to_upd_core = dataframe_to_update[['link', 'name', 'location', 'level', 'company', 'salary',
                                                             'description', 'date_created', 'date_of_download',
                                                             'status']]

                if not dataframe_to_upd_core.empty:
                    try:
                        log.info(f'Обновляем таблицу core_fact_table.')
                        core_fact_data_tuples = [tuple(x) for x in dataframe_to_upd_core.to_records(index=False)]
                        cols = ",".join(dataframe_to_upd_core.columns)
                        self.client.execute(f"INSERT INTO {config['database']}.core_fact_table ({cols}) VALUES",
                                            core_fact_data_tuples)
                        self.log.info(f"Количество строк вставлено в core_fact_table: "
                                      f"{len(core_fact_data_tuples)}, обновлена таблица core_fact_table "
                                      f"в БД {config['database']}.")

                    except Exception as e:
                        self.log.error(f"Ошибка при вставке записей из dataframe_to_upd_core: {e}")
                        self.log.error(f"Содержимое dataframe_to_upd_core: {dataframe_to_upd_core.columns}")
                        raise

                else:
                    self.log.info(
                        f"Обновленных вакансий VK нет, таблица 'core_fact_table' в БД {config['database']} не изменена.")

                dataframe_to_delete = dataframe_to_closed[['link', 'name', 'location', 'level', 'company', 'salary',
                                                           'description', 'date_created', 'date_of_download', 'status']]

                # Код для удаления "closed" записей в таблице core_fact_table
                if not dataframe_to_delete.empty:
                    try:
                        log.info(f'Удаляем из core_fact_table закрытые вакансии.')
                        dataframe_to_delete_tuples = [tuple(x) for x in
                                                      dataframe_to_delete[['link']].to_records(index=False)]
                        for to_delete in dataframe_to_delete_tuples:
                            self.client.execute(
                                f"DELETE FROM {config['database']}.core_fact_table WHERE link = '{to_delete[0]}'")
                        self.log.info(f"Количество строк удалено из core_fact_table: "
                                      f"{len(dataframe_to_delete_tuples)}, обновлена таблица core_fact_table в БД "
                                      f"{config['database']}.")

                    except Exception as e:
                        self.log.error(f"Ошибка при вставке записей из dataframe_to_delete: {e}")
                        self.log.error(f"Содержимое dataframe_to_upd_core: {dataframe_to_delete.columns}")
                        raise

                else:
                    self.log.info(f"Таблица 'core_fact_table' не изменена, нет строк для удаления.")

            else:
                self.log.info(f"Не найдено вакансий при парсинге данных.")

        except Exception as e:
            self.log.error(f"Ошибка при сохранении данных в функции 'save_df': {e}")
            raise


def run_vk_parser(**context):
    """
    Основной вид задачи для запуска парсера для вакансий VK
    """
    log = context['ti'].log
    log.info('Запуск парсера ВК')
    try:
        try:
            parser = VKJobParser(url_vk, profs, log, conn)
            parser.find_vacancies()
            parser.find_values_in_db()
            parser.find_vacancies_description()
            parser.save_df()
            parser.stop()
            log.info('Парсер ВК успешно провел работу')
        except Exception as e:
            log.error(f'Ошибка во время работы парсера ВК: {e}')
    except Exception as e_outer:
        log.error(f'Исключение в функции run_vk_parser: {e_outer}')

# def run_sber_parser(**context):
#     """
#     Основной вид задачи для запуска парсера для вакансий Sber
#     """
#     log = context['ti'].log
#     log.info('Запуск парсера Сбербанка')
#     try:
#         try:
#             parser = SberJobParser(url_sber, profs, log, client)
#             parser.find_vacancies()
#             parser.find_vacancies_description()
#             parser.save_df()
#             parser.stop()
#             log.info('Парсер Сбербанка успешно провел работу')
#         except Exception as e:
#             log.error(f'Ошибка во время работы парсера Сбер: {e}')
#     except Exception as e_outer:
#         log.error(f'Исключение в функции run_sber_parser: {e_outer}')
#
# def run_tin_parser(**context):
#     """
#     Основной вид задачи для запуска парсера для вакансий Tinkoff
#     """
#     log = context['ti'].log
#     log.info('Запуск парсера Тинькофф')
#     try:
#         try:
#             parser = TinkoffJobParser(url_vk, profs, log, client)
#             parser.open_all_pages()
#             parser.all_vacs_parser()
#             parser.find_vacancies_description()
#             parser.save_df()
#             parser.stop()
#             log.info('Парсер Тинькофф успешно провел работу')
#         except Exception as e:
#             log.error(f'Ошибка во время работы парсера Тинькофф: {e}')
#     except Exception as e_outer:
#         log.error(f'Исключение в функции run_tin_parser: {e_outer}')


hello_bash_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "Желаю удачного парсинга! Да прибудет с нами безотказный интернет!"')


parse_vkjobs = PythonOperator(
    task_id='parse_vkjobs',
    python_callable=run_vk_parser,
    provide_context=True,
    dag=updated_raw_dag
)
#
# parse_sber = PythonOperator(
#     task_id='parse_sber',
#     python_callable=run_sber_parser,
#     provide_context=True,
#     dag=daily_raw_dag
# )
#
# parse_tink = PythonOperator(
#     task_id='parse_tink',
#     python_callable=run_tin_parser,
#     provide_context=True,
#     dag=daily_raw_dag
# )


end_task = DummyOperator(
    task_id="end_task"
)

hello_bash_task >> parse_vkjobs >> end_task
# hello_bash_task >> parse_vkjobs >> parse_sber >> parse_tink >> end_task