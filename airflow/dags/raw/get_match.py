from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import logging
from airflow.utils.task_group import TaskGroup
import logging
import time
from datetime import datetime
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.utils.dates import days_ago
from psycopg2.extras import execute_values
import re

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.variables_settings import variables, base_getmatch

table_name = variables['raw_tables'][6]['raw_tables_name']
url = base_getmatch

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)

log = logging

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    'start_date': days_ago(1)
}

from raw.base_job_parser import BaseJobParser

class GetMatchJobParser(BaseJobParser):
    def find_vacancies(self):
        self.items = []
        HEADERS = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",
                }
        self.log.info(f'Создаем пустой список')
        self.items = []
        # seen_ids = set()
        self.all_links = []  
        self.log.info(f'Парсим данные')
        # Парсим линки вакансий с каждой страницы (на сайте примерно 50страниц, 100 - это с избытком)     
        
        for i in range(1, 100):
            response = requests.get(url.format(i=i), headers=HEADERS)
            soup = BeautifulSoup(response.content, 'html.parser')
            divs = soup.find_all('div', class_='b-vacancy-card-title')
            for div in divs:
                vacancy_url = 'https://getmatch.ru/' + div.find('a').get('href')
                self.all_links.append(vacancy_url)

        vacancy_count = 0
        for link in self.all_links:
            if vacancy_count < 5:
                resp = requests.get(link, HEADERS)
                vac = BeautifulSoup(resp.content, 'lxml')

                try:
                    # парсим грейд вакансии
                    term_element = vac.find('div', {'class': 'col b-term'}, text='Уровень')
                    level = term_element.find_next('div', {'class': 'col b-value'}).text.strip() if term_element else None

                    # Парсим иностранные языки
                    lang = vac.find('div', {'class': 'col b-term'}, text='Английский')
                    if lang is not None:
                        level_lang= lang.find_next('span', {'class': 'b-language-description d-md-none'}).text.strip()
                        lang=lang.text.strip()
                        language = f"{lang}: {level_lang}"
                        if level==level_lang:
                            level=None
                    else:
                        language=None

                    # Парсим навыки
                    stack_container = vac.find('div', class_='b-vacancy-stack-container')
                    if stack_container is not None:
                        labels = stack_container.find_all('span', class_='g-label')
                        page_stacks = ', '.join([label.text.strip('][') for label in labels])
                    else:
                        page_stacks=None

                    # Парсим описание вакансий
                    description_element = vac.find('section', class_='b-vacancy-description')
                    description_lines = description_element.stripped_strings
                    description = '\n'.join(description_lines)

                    # Парсим и распаршиваем зарплаты
                    salary_text = vac.find('h3').text.strip()
                    salary_text = salary_text.replace('\u200d', '-').replace('—', '-')
                    if '₽/мес на руки' in vac.find('h3').text:
                        salary_parts = list(map(str.strip, salary_text.split('-')))
                        salary_from = salary_parts[0]
                        if len(salary_parts) == 1:
                            salary_to = None if 'от' in vac.find('h3').text else salary_parts[0]
                        elif len(salary_parts) > 1:
                            salary_to = salary_parts[2]
                    else:
                        salary_from = None
                        salary_to = None

                    # Приводим зарплаты к числовому формату
                    if salary_from is not None:
                        numbers = re.findall(r'\d+', salary_from)
                        combined_number = ''.join(numbers)
                        salary_from = int(combined_number) if combined_number else None
                    if salary_to is not None:
                        numbers = re.findall(r'\d+', salary_to)
                        combined_number = ''.join(numbers)
                        salary_to = int(combined_number) if combined_number else None

                    # Парсим формат работы
                    job_form_classes = ['g-label-linen', 'g-label-zanah', 'ng-star-inserted']
                    job_form = vac.find('span', class_=job_form_classes)
                    job_format = job_form.get_text(strip=True) if job_form is not None else None

                    # Получаем другие переменные
                    date_created = date_of_download = datetime.now().date()
                    status ='existing'
                    version_vac=1
                    actual=1

                    item = {
                        "company": vac.find('h2').text.replace('\xa0', ' ').strip('в'),
                        "vacancy_name": vac.find('h1').text,
                        "skills": page_stacks,
                        "towns": ', '.join([span.get_text(strip=True).replace('📍', '') for span in vac.find_all('span', class_='g-label-secondary')]),
                        "vacancy_url": link,
                        "description": description,
                        "job_format": job_format,
                        "level": level,
                        "salary_from": salary_from,
                        "salary_to": salary_to,
                        "date_created": date_created,
                        "date_of_download": date_of_download,
                        "source_vac": 1,
                        "status": status,
                        "version_vac": version_vac,
                        "actual": actual,
                        "languages":language,
                        }
                    print(f"Adding item: {item}")
                    item_df = pd.DataFrame([item])
                    self.df = pd.concat([self.df, item_df], ignore_index=True)
                    time.sleep(3)
                    vacancy_count += 1
                except AttributeError as e:
                    print(f"Error processing link {link}: {e}")
            else:
                break
        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")

    # def save_df(self):
    #     self.log.info(f"Осуществляем загрузку данных в БД")
    #     try:
    #         if not self.df.empty:
    #             data = [tuple(x) for x in self.df.to_records(index=False)]
    #             query = f"""
    #                 INSERT INTO {self.schema}.{table_name}
    #                    (vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, exp_from, exp_to,
    #                    description, job_type, job_format, languages, skills, source_vac, date_created, date_of_download,
    #                    status, date_closed, version_vac, actual)
    #                 VALUES %s
    #                 ON CONFLICT (vacancy_url, version_vac) DO UPDATE SET
    #                 vacancy_name = EXCLUDED.vacancy_name,
    #                 towns = EXCLUDED.towns,
    #                 level = EXCLUDED.level,
    #                 company = EXCLUDED.company,
    #                 salary_from = EXCLUDED.salary_from,
    #                 salary_to = EXCLUDED.salary_to,
    #                 exp_from = EXCLUDED.exp_from,
    #                 exp_to = EXCLUDED.exp_to,
    #                 description = EXCLUDED.description,
    #                 job_type = EXCLUDED.job_type,
    #                 job_format = EXCLUDED.job_format,
    #                 languages = EXCLUDED.languages,
    #                 skills = EXCLUDED.skills,
    #                 source_vac = EXCLUDED.source_vac,
    #                 date_created = EXCLUDED.date_created,
    #                 date_of_download = EXCLUDED.date_of_download,
    #                 status = EXCLUDED.status,
    #                 date_closed = EXCLUDED.date_closed,
    #                 version_vac = EXCLUDED.version_vac,
    #                 actual = EXCLUDED.actual;"""
    #             self.log.info(f"Запрос вставки данных: {query}")
    #             print(self.df.head())
    #             self.log.info(self.df.head())
    #             execute_values(self.cur, query, data)
    #             self.conn.commit()
    #             self.log.info("Общее количество загруженных в БД вакансий: " + str(len(self.df)) + "\n")
    #     except Exception as e:
    #         self.log.error(f"Произошла ошибка при сохранении данных в функции 'save_df': {e}")
    #         raise
    #     #
    #     # finally:
    #     #     self.cur.close()
    #     #     self.conn.close()

    # def generating_dataframes(self):
    #     """
    #     Method for generating dataframes for data updates
    #     """
    #     try:
    #         if not self.df.empty:
    #             self.log.info(f"Проверка типов данных в DataFrame: \n {self.df.dtypes}")
    #
    #             self.log.info('Собираем вакансии для сравнения')
    #             query = f"""SELECT DISTINCT vacancy_url FROM {self.schema}.{table_name}"""
    #             self.cur.execute(query)
    #             links_in_db = self.cur.fetchall()
    #             links_in_db_set = set(vacancy_url for vacancy_url, in links_in_db)
    #             links_in_parsed = set(self.df['vacancy_url'])
    #             links_to_close = links_in_db_set - links_in_parsed
    #
    #             self.log.info('Создаем датафрейм dataframe_to_closed')
    #             if links_to_close:
    #                 for link in links_to_close:
    #                     query = f"""
    #                         SELECT vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, exp_from,
    #                         exp_to, description, job_type, job_format, languages, skills, source_vac, date_created,
    #                         date_of_download, status, date_closed, version_vac, actual
    #                         FROM {self.schema}.{table_name}
    #                         WHERE vacancy_url = '{link}'
    #                             AND status != 'closed'
    #                             AND actual != '-1'
    #                             AND version_vac = (
    #                                 SELECT max(version_vac) FROM {self.schema}.{table_name}
    #                                 WHERE vacancy_url = '{link}'
    #                             )
    #                         ORDER BY date_of_download DESC, version_vac DESC
    #                         LIMIT 1
    #                         """
    #                     self.cur.execute(query)
    #                     records_to_close = self.cur.fetchall()
    #
    #                     if records_to_close:
    #                         for record in records_to_close:
    #                             data_to_close = {
    #                                 'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
    #                                 'level': record[3], 'company': record[4], 'salary_from': record[5],
    #                                 'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
    #                                 'description': record[9], 'job_type': record[10], 'job_format': record[11],
    #                                 'languages': record[12], 'skills': record[13], 'source_vac': record[14],
    #                                 'date_created': record[15], 'date_of_download': datetime.now().date(),
    #                                 'status': 'closed', 'date_closed': datetime.now().date(),
    #                                 'version_vac': record[-2] + 1, 'actual': -1
    #                             }
    #                             self.dataframe_to_closed = pd.concat([self.dataframe_to_closed,
    #                                                                   pd.DataFrame(data_to_close, index=[0])])
    #                 self.log.info('Датафрейм dataframe_to_closed создан')
    #             else:
    #                 self.log.info('Список links_to_close пуст')
    #
    #             self.log.info('Присваиваем статусы изменений')
    #             data = [tuple(x) for x in self.df.to_records(index=False)]
    #             for record in data:
    #                 link = record[0]
    #                 query = f"""
    #                     SELECT vacancy_url, vacancy_name, towns, level, company, salary_from, salary_to, exp_from,
    #                         exp_to, description, job_type, job_format, languages, skills, source_vac, date_created,
    #                         date_of_download, status, date_closed, version_vac, actual
    #                     FROM {self.schema}.{table_name}
    #                     WHERE vacancy_url = '{link}'
    #                     ORDER BY date_of_download DESC, version_vac DESC
    #                     LIMIT 1
    #                     """
    #                 self.cur.execute(query)
    #                 records_in_db = self.cur.fetchall()
    #
    #                 if records_in_db:
    #                     for old_record in records_in_db:
    #                         old_status = old_record[-4]
    #                         next_version = old_record[-2] + 1
    #
    #                         if old_status == 'new':
    #                             data_new_vac = {
    #                                 'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
    #                                 'level': record[3], 'company': record[4], 'salary_from': record[5],
    #                                 'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
    #                                 'description': record[9], 'job_type': record[10], 'job_format': record[11],
    #                                 'languages': record[12], 'skills': record[13], 'source_vac': record[14],
    #                                 'date_created': old_record[15], 'date_of_download': datetime.now().date(),
    #                                 'status': 'existing', 'date_closed': old_record[-3], 'version_vac': next_version,
    #                                 'actual': 1
    #                             }
    #                             self.dataframe_to_update = pd.concat(
    #                                 [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
    #                             )
    #
    #                         elif old_status == 'existing':
    #                             if pd.Series(old_record[:13]).equals(pd.Series(record[:13])):
    #                                 pass
    #
    #                             else:
    #                                 data_new_vac = {
    #                                     'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
    #                                     'level': record[3], 'company': record[4], 'salary_from': record[5],
    #                                     'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
    #                                     'description': record[9], 'job_type': record[10], 'job_format': record[11],
    #                                     'languages': record[12], 'skills': record[13], 'source_vac': record[14],
    #                                     'date_created': old_record[15], 'date_of_download': datetime.now().date(),
    #                                     'status': 'existing', 'date_closed': old_record[-3],
    #                                     'version_vac': next_version, 'actual': 1
    #                                 }
    #                                 self.dataframe_to_update = pd.concat(
    #                                     [self.dataframe_to_update, pd.DataFrame(data_new_vac, index=[0])]
    #                                 )
    #                         elif old_status == 'closed':
    #                             if link in links_in_parsed:
    #                                 data_clos_new = {
    #                                     'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
    #                                     'level': record[3], 'company': record[4], 'salary_from': record[5],
    #                                     'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
    #                                     'description': record[9], 'job_type': record[10], 'job_format': record[11],
    #                                     'languages': record[12], 'skills': record[13], 'source_vac': record[14],
    #                                     'date_created': record[15], 'date_of_download': datetime.now().date(),
    #                                     'status': 'new', 'date_closed': record[-3], 'version_vac': next_version,
    #                                     'actual': 1
    #                                 }
    #                                 self.dataframe_to_update = pd.concat(
    #                                     [self.dataframe_to_update, pd.DataFrame(data_clos_new, index=[0])]
    #                                 )
    #                 else:
    #                     data_full_new = {
    #                         'vacancy_url': link, 'vacancy_name': record[1], 'towns': record[2],
    #                         'level': record[3], 'company': record[4], 'salary_from': record[5],
    #                         'salary_to': record[6], 'exp_from': record[7], 'exp_to': record[8],
    #                         'description': record[9], 'job_type': record[10], 'job_format': record[11],
    #                         'languages': record[12], 'skills': record[13], 'source_vac': record[14],
    #                         'date_created': record[15], 'date_of_download': datetime.now().date(),
    #                         'status': 'new', 'date_closed': record[-3], 'version_vac': 1,
    #                         'actual': 1
    #                     }
    #                     self.dataframe_to_update = pd.concat(
    #                         [self.dataframe_to_update, pd.DataFrame(data_full_new, index=[0])]
    #                     )
    #
    #     except Exception as e:
    #         self.log.error(f"Ошибка при работе метода 'generating_dataframes': {e}")
    #         raise

    # def update_database_queries(self):
    #     """
    #     Method for performing data update in the database
    #     """
    #     self.log.info('Старт обновления данных в БД')
    #     try:
    #         if not self.dataframe_to_update.empty:
    #             data_tuples_to_insert = [tuple(x) for x in self.dataframe_to_update.to_records(index=False)]
    #             cols = ",".join(self.dataframe_to_update.columns)
    #             self.log.info(f'Обновляем таблицу {table_name}.')
    #             query = f"""INSERT INTO {self.schema}.{table_name} ({cols}) VALUES ({", ".join(["%s"] *
    #                         len(self.dataframe_to_update.columns))})"""
    #             self.log.info(f"Запрос вставки данных: {query}")
    #             self.cur.executemany(query, data_tuples_to_insert)
    #             self.log.info(f"Количество строк вставлено в {self.schema}.{table_name}: "
    #                           f"{len(data_tuples_to_insert)}, обновлена таблица {self.schema}.{table_name} "
    #                           f"в БД.")
    #
    #         if not self.dataframe_to_closed.empty:
    #             self.log.info(f'Добавляем строки удаленных вакансий в таблицу {table_name}.')
    #             data_tuples_to_closed = [tuple(x) for x in self.dataframe_to_closed.to_records(index=False)]
    #             cols = ",".join(self.dataframe_to_closed.columns)
    #             query = f"""INSERT INTO {self.schema}.{table_name} ({cols}) VALUES ({", ".join(["%s"] *
    #                         len(self.dataframe_to_closed.columns))})"""
    #             self.log.info(f"Запрос вставки данных: {query}")
    #             self.cur.executemany(query, data_tuples_to_closed)
    #             self.log.info(f"Количество строк удалено из {self.schema}.{table_name}: "
    #                           f"{len(data_tuples_to_closed)}, обновлена таблица {self.schema}.{table_name} в БД.")
    #         else:
    #             self.log.info(f"dataframe_to_closed пуст.")
    #
    #         self.conn.commit()
    #         self.log.info(f"Операции успешно выполнены. Изменения сохранены в таблицах.")
    #     except Exception as e:
    #         self.conn.rollback()
    #         self.log.error(f"Произошла ошибка: {str(e)}")
    #     finally:
    #         self.cur.close()

# def run_init_getmatch_parser():
#     """
#     Основной вид задачи для запуска парсера для вакансий GetMatch
#     """
#     # log = context['ti'].log
#     log.info('Запуск парсера GetMatch')
#     try:
#         parser = GetMatchJobParser(url, log, conn, table_name)
#         parser.find_vacancies()
#         parser.addapt_numpy_null()
#         parser.save_df()
#         log.info('Парсер GetMatch успешно провел работу')
#     except Exception as e:
#         log.error(f'Ошибка во время работы парсера GetMatch: {e}')
#
#
# with DAG(dag_id="initial_getmatch_parser",
#          schedule_interval=None, tags=['admin_1T'],
#          default_args=default_args,
#          catchup=False) as dag_initial_getmatch_parser:
#     parse_get_match_jobs = PythonOperator(
#         task_id='init_run_getmatch_parser_task',
#         python_callable=run_init_getmatch_parser,
#         provide_context=True)
#
# parse_get_match_jobs
#
# def run_update_getmatch_parser():
#     """
#     Основной вид задачи для запуска парсера для вакансий GetMatch
#     """
#     # log = context['ti'].log
#     log.info('Запуск парсера GetMatch')
#     try:
#         parser = GetMatchJobParser(url, log, conn, table_name)
#         parser.find_vacancies()
#         parser.generating_dataframes()
#         parser.addapt_numpy_null()
#         parser.update_database_queries()
#         # parser.stop()
#         log.info('Парсер GetMatch успешно провел работу')
#     except Exception as e:
#         log.error(f'Ошибка во время работы парсера GetMatch: {e}')
#
# with DAG(dag_id="update_getmatch_parser",
#          schedule_interval=None,
#          tags=['admin_1T'],
#          default_args=default_args,
#          catchup=False) as dag_update_getmatch_parser:
#     update_getmatch_parser_job = PythonOperator(
#         task_id='update_getmatch_parser_task',
#         python_callable=run_update_getmatch_parser,
#         provide_context=True)
#
# dag_update_getmatch_parser