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

from variables_settings import variables, profs
from raw.base_job_parser_selenium import BaseJobParserSelenium

table_name = variables['raw_tables'][0]['raw_tables_name']

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

class VKJobParser(BaseJobParserSelenium):
    """
    Парсер для вакансий с сайта VK, наследованный от BaseJobParser
    """
    def find_vacancies(self):
        """
        Метод для нахождения вакансий с VK
        """
        print('Старт парсинга вакансий VK')
        self.browser.implicitly_wait(3)
        # Поиск и запись вакансий на поисковой странице
        for prof in self.profs:
            text_str = self.url + '?search=' + str(prof.replace(' ', '+')).lower()
            self.browser.get(text_str)
            self.browser.maximize_window()
            self.browser.delete_all_cookies()
            time.sleep(5)

            # Прокрутка вниз до конца страницы
            self.scroll_down_page()

            try:
                # vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div[1]/div[2]/div/div')
                vacs = self.browser.find_elements(By.CLASS_NAME, 'vacancy_vacancyItem__jrNqL')
                vacs = [div for div in vacs if 'vacancy_vacancyItem__jrNqL' in str(div.get_attribute('class'))]
                print(f"Парсим вакансии по запросу: {prof}")
                print(f"Количество: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['vacancy_url'] = vac.get_attribute('href')
                    print(vac_info['vacancy_url'])
                    vac_info['vacancy_name'] = vac.find_element(By.CLASS_NAME, 'vacancy_vacancyItemTitleWrapper__AFMJr').text
                    print(vac_info['vacancy_name'])
                    location_and_company = vac.find_element(By.CLASS_NAME, 'vacancy_vacancyItemDescriptionText__ntfGz')
                    location_and_company = location_and_company.find_elements(By.TAG_NAME, 'div')
                    vac_info['towns'] = location_and_company[0].text
                    print(vac_info['towns'])
                    vac_info['company'] = location_and_company[1].text
                    print(vac_info['company'])
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                print(f"Произошла ошибка: {e}")

        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий в Yandex после удаления дубликатов: " +
                      str(len(self.df)) + "\n")
        self.df['date_created'] = datetime.now().date()
        self.df['date_of_download'] = datetime.now().date()
        self.df['source_vac'] = 10
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
                    vacancy_url = self.df.loc[descr, 'vacancy_url']
                    self.browser.get(vacancy_url)
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
                    self.log.error(f"Произошла ошибка: {e}, ссылка {self.df.loc[descr, 'vacancy_url']}")
                    pass
        else:
            self.log.info(f"Нет вакансий для парсинга")
