from airflow.utils.task_group import TaskGroup
import logging
import time
from datetime import datetime
from airflow.utils.dates import days_ago
from selenium.webdriver.common.by import By
import numpy as np

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from variables_settings import variables
from raw.base_job_parser_selenium import BaseJobParserSelenium

table_name = variables['raw_tables'][4]['raw_tables_name']

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)
log = logging

# Default parameters
default_args = {
    "owner": "admin_1T",
    'start_date': days_ago(1)
}

class RemoteJobParser(BaseJobParserSelenium):
    """
    Парсер вакансий с сайта RemoteJob, наследованный от BaseJobParserSelenium
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
                        salary_to = np.nan
                    elif 'до' in cleaned_salary_info:
                        salary_from = np.nan
                        salary_to = int(cleaned_salary_info.split('до')[-1])
                    else:
                        salary_from = np.nan
                        salary_to = np.nan
                except Exception as e:
                    self.log.error(f"Ошибка при обработке информации о зарплате: {e}")
                    salary_from = np.nan
                    salary_to = np.nan

            vacancy_link = div.find_element(By.TAG_NAME, 'a').get_attribute('href')
            vacancy_name = div.find_element(By.CSS_SELECTOR, '.navbar li, a, button').text
            company = div.find_elements(By.TAG_NAME, 'small')[1].text.strip()

            vacancy_data['vacancy_link'] = vacancy_link
            vacancy_data['vacancy_name'] = vacancy_name
            vacancy_data['company'] = company
            vacancy_data['salary_from'] = salary_from
            vacancy_data['salary_to'] = salary_to
            self.url_l.append(vacancy_data)

    def all_vacs_parser(self):
        """
        Метод для нахождения вакансий с Tinkoff
        """
        try:
            self.browser.implicitly_wait(3)

            vacs = self.browser.find_elements(By.CLASS_NAME, 'eM3bvP')
            for vac in vacs:
                try:
                    vac_info = {}
                    vac_info['vacancy_url'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    data = vac.find_elements(By.CLASS_NAME, 'gM3bvP')
                    vac_info['vacancy_name'] = data[0].text
                    vac_info['level'] = data[1].text
                    vac_info['towns'] = data[2].text
                    self.df.loc[len(self.df)] = vac_info
                except Exception as e:
                    log.error(f"Произошла ошибка: {e}, ссылка на вакансию: {vac_info['vacancy_url']}")

            self.df = self.df.drop_duplicates()
            self.log.info("Общее количество найденных вакансий после удаления дубликатов: "
                          + str(len(self.df)) + "\n")
            self.df['company'] = 'Тинькофф'
            self.df['date_created'] = datetime.now().date()
            self.df['date_of_download'] = datetime.now().date()
            self.df['source_vac'] = 8
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
                    vacancy_url = self.df.loc[descr, 'vacancy_url']
                    self.browser.get(vacancy_url)
                    self.browser.delete_all_cookies()
                    self.browser.implicitly_wait(3)
                    desc = str(self.browser.find_element(By.CLASS_NAME, 'dyzaXu').text)
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = desc

                    self.log.info(f"Описание успешно добавлено для вакансии {descr + 1}")

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'vacancy_url']}")
                    pass
        else:
            self.log.info(f"Нет описания вакансий для парсинга")
