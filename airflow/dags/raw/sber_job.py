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

from raw.variables_settings import variables, base_sber
from raw.base_job_parser_selenium import BaseJobParserSelenium

table_name = variables['raw_tables'][1]['raw_tables_name']

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

class SberJobParser(BaseJobParserSelenium):

    def find_vacancies(self):
        """
        Finds and records vacancies on the search page.
        """
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
                    vac_info['vacancy_url'] = vac.find_element(By.TAG_NAME, 'a').get_attribute('href')
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
        self.df['source_vac'] = 7
        self.df['date_created'] = self.df['date_created'].apply(lambda x: dateparser.parse(x, languages=['ru']))
        self.df['date_created'] = pd.to_datetime(self.df['date_created']).dt.to_pydatetime()
        self.df['date_created'] = self.df['date_created'].dt.date
        self.df['date_of_download'] = datetime.now().date()
        self.df['status'] = 'existing'
        self.df['description'] = None
        self.df['version_vac'] = 1
        self.df['actual'] = 1

    def find_vacancies_description(self):
        """
        Метод для парсинга описаний вакансий для SberJobParser.
        """
        self.log.info('Старт парсинга описаний вакансий')
        if not self.df.empty:
            for descr in self.df.index:
                try:
                    vacancy_url = self.df.loc[descr, 'vacancy_url']
                    self.browser.get(vacancy_url)
                    self.browser.delete_all_cookies()
                    time.sleep(3)
                    desc = self.browser.find_element(By.CLASS_NAME, 'section').text
                    desc = desc.replace(';', '')
                    self.df.loc[descr, 'description'] = str(desc)

                except Exception as e:
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'vacancy_url']}")
                    pass
        else:
            self.log.info(f"Нет вакансий для парсинга")
