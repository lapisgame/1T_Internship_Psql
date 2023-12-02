from airflow.utils.task_group import TaskGroup
import logging
import time
from datetime import datetime
from airflow.utils.dates import days_ago
from selenium.webdriver.common.by import By
import pandas as pd
import dateparser

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.variables_settings import variables
from raw.base_job_parser_selenium import BaseJobParserSelenium

table_name = variables['raw_tables'][2]['raw_tables_name']

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

class TinkoffJobParser(BaseJobParserSelenium):
    """
    Парсер вакансий с сайта Tinkoff, наследованный от BaseJobParserSelenium
    """
    def open_all_pages(self):
        self.log.info('Работает функция open_all_pages')
        self.browser.implicitly_wait(10)
        elements = self.browser.find_elements(By.CLASS_NAME, 'fuBQPo')
        for element in elements:
            element.click()
        self.log.info('Aункция open_all_pages успешно завершена')

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
