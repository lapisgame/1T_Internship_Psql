from airflow.utils.task_group import TaskGroup
import logging
import time
from datetime import datetime
from airflow.utils.dates import days_ago
from selenium.webdriver.common.by import By

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.variables_settings import variables, base_vk
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

    def find_vacancies(self):
        """
        Finds and records vacancies on the search page.
        """
        self.browser.implicitly_wait(3)

        for prof in self.profs:
            # Enter search query
            input_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/div/div/div/input')
            input_button.send_keys(prof['fullName'])

            # Click search button
            click_button = self.browser.find_element(By.XPATH,
                                                     '/html/body/div/div[1]/div[1]/div/form/div[1]/div[4]/div/button')
            click_button.click()
            time.sleep(5)

            # Scroll down to the end of the page
            self.scroll_down_page()

            try:
                # Find and parse vacancies
                vacs_bar = self.browser.find_element(By.XPATH, '/html/body/div/div[1]/div[2]/div/div')
                vacs = vacs_bar.find_elements(By.CLASS_NAME, 'result-item')
                vacs = [div for div in vacs if 'result-item' in str(div.get_attribute('class'))]
                self.log.info(f"Parsing vacancies for query: {prof['fullName']}")
                self.log.info(f"Number of vacancies: " + str(len(vacs)) + "\n")

                for vac in vacs:
                    vac_info = {}
                    vac_info['vacancy_url'] = str(vac.get_attribute('href'))
                    vac_info['vacancy_name'] = str(vac.find_element(By.CLASS_NAME, 'title-block').text)
                    vac_info['towns'] = str(vac.find_element(By.CLASS_NAME, 'result-item-place').text)
                    vac_info['company'] = str(vac.find_element(By.CLASS_NAME, 'result-item-unit').text)
                    self.df.loc[len(self.df)] = vac_info

            except Exception as e:
                self.log.error(f"An error occurred: {e}")
                input_button.clear()

        self.df = self.df.drop_duplicates()
        self.log.info("Total number of found vacancies after removing duplicates: " + str(len(self.df)) + "\n")
        self.df['date_created'] = datetime.now().date()
        self.df['date_of_download'] = datetime.now().date()
        self.df['source_vac'] = 9
        self.df['status'] = 'existing'
        self.df['version_vac'] = 1
        self.df['actual'] = 1

    def find_vacancies_description(self):
        """
        Starts parsing vacancy descriptions.
        """

        self.log.info('Start parsing vacancy descriptions')

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
                    self.log.error(f"An error occurred: {e}, link {self.df.loc[descr, 'vacancy_url']}")
                    pass
        else:
            self.log.info(f"No vacancies to parse")