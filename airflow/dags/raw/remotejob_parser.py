from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.keys import Keys
from fake_useragent import UserAgent
import logging
import time
from datetime import datetime
import dateparser
from airflow.utils.dates import days_ago
from selenium.webdriver.common.by import By
import numpy as np
import pandas as pd

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
options = ChromeOptions()

# Default parameters
default_args = {
    "owner": "admin_1T",
    'start_date': days_ago(1)
}

class RemoteJobParser(BaseJobParserSelenium):
    """
    Parser for job vacancies from the RemoteJob website, inherited from BaseJobParserSelenium
    """
    def main_page(self, url):
        self.log.info(f'Analyzing the main page {url}')
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
                    self.log.error(f"Error processing salary information: {e}")
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

    def find_vacancies_description(self):
        # initializing the main data dictionary
        for vacancy in self.url_l:
            # self.log.info(f'URL {vacancy["vacancy_link"]}')
            try:
                self.browser.get(vacancy["vacancy_link"])
                date_created = dateparser.parse(
                    self.wait.until(EC.presence_of_element_located((By.XPATH, "//*[@class='text-left']"))).text.strip(),
                    languages=['ru']).date()

            except (WebDriverException, TimeoutException) as e:
                # Логируем ошибку
                self.log.error(f'Failed to load page {vacancy["vacancy_link"]}: {e}')
                # Пробуем перезапустить браузер
                self.restart_browser()
                # Пропускаем текущую вакансию
                continue
            # except:
            #     date_created = datetime.now().date()

            try:
                text_tag = self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@class="row p-y-3"]')))
                text = "\n".join((text_tag.text.strip().split("\n")[2:])).replace('\r', '')
                description = text[:text.find('Откликнуться на вакансию')].strip().replace(
                    'Контактная информация работодателя станет доступна сразу после того, как вы оставите свой отклик на эту вакансию.',
                    '')
            except TimeoutException:
                # If the exception is raised, skip the current iteration and move on to the next vacancy.
                self.log.error(
                    f"Failed to find a text element on the page {vacancy['vacancy_link']}. The page will be skipped.")
                continue

            self.df = pd.concat([self.df, pd.DataFrame({
                'vacancy_url': [vacancy["vacancy_link"]],
                'vacancy_name': [vacancy["vacancy_name"]],
                'company': [vacancy["company"]],
                'salary_from': [vacancy["salary_from"]],
                'salary_to': [vacancy["salary_to"]],
                'description': [description],
                'job_format': ['Удаленно'],
                'source_vac': [6],
                'date_created': [date_created],
                'date_of_download': [datetime.now().date()],
                'status': ['existing'],
                'version_vac': [1],
                'actual': [1]
            })], ignore_index=True)
            time.sleep(3)

    def find_vacancies(self):
        self.wait = WebDriverWait(self.browser, 10)
        self.url_l = []
        options.add_argument('--headless')
        ua = UserAgent().chrome
        self.headers = {'User-Agent': ua}
        options.add_argument(f'--user-agent={ua}')

        self.log.info('Starting parsing job vacancies from Remote-Job...')

        for prof in self.profs:
            self.log.info(f'Start parsing job vacancy: "{prof}"')
            try:
                self.browser.get(self.url)
                time.sleep(10)
                # search and process vacancies operations
            except Exception as e:
                self.log.error(f"Error processing job vacancy {prof}: {e}")
                continue
            try:
                search = self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="search_query"]')))
                search.send_keys(prof)
                search.send_keys(Keys.ENTER)
            except NoSuchElementException:
                self.log.error(f"No such element: Unable to locate element: for profession {prof}")
                continue

            if self.browser.find_element(By.CSS_SELECTOR, '.h2, h2').text == 'Vacancies not found':
                continue
            try:
                last_page = int(self.browser.find_element(By.CLASS_NAME, 'pagination').text.split('\n')[-2])
            except NoSuchElementException:
                last_page = 2

            vacancy_url = self.browser.current_url

            self.log.info(f'Pages to process: {last_page}')

            for i in range(1, last_page + 1):
                self.log.info(f'Processing page {i}/{last_page}.')
                vacancy_url_for_page = f'{vacancy_url}&page={i}'
                self.main_page(vacancy_url_for_page)
                self.find_vacancies_description()
                self.url_l = []
                self.log.info(f'Page {i} processed!')

            # Add browser refresh for each new vacancy
            self.browser.refresh()
            time.sleep(5)  # Pause after refreshing the page to ensure it is fully loaded