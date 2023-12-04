from airflow.utils.task_group import TaskGroup
import logging
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from variables_settings import variables, base_careerspace
from raw.base_job_parser import BaseJobParser

table_name = variables['raw_tables'][9]['raw_tables_name']

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)

log = logging

default_args = {
    "owner": "admin_1T",
    'start_date': days_ago(1)
}

class CareerspaceJobParser(BaseJobParser):
    def find_vacancies(self):
        """
        This method parses job vacancies from the Careerspace website.
        It iterates through different pages and job levels to retrieve job details such as company name, vacancy name,
        vacancy URL, job format, salary range, date created, date of download, source of vacancy, status, version of vacancy,
        actual status, description, towns, and level of experience.
        The parsed data is stored in a pandas DataFrame.

        Returns:
            None
        """
        HEADERS = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",
        }
        url_template = 'https://careerspace.app/api/v2/jobs/filters?skip={i}&take=8&sortBy=new-desc&functions=8%2C13%2C14%2C9&jobLevel%5B0%5D={o}&currencyCode=RUR'
        # i - page number, o - experience level
        on = ['intern', 'specialist', 'manager', 'head', 'director']

        self.log.info(f'Creating an empty list')
        self.all_items = []
        self.unique_items = []
        self.items = []
        seen_ids = set()
        self.log.info(f'Parsing data')

        # Parse job links from each page
        for o in on:
            page = 1
            # Iterate until there are pages
            while True:
                i = page * 8  # 8 - number of vacancies per page (default)
                url = base_careerspace.format(i=i, o=o)  # Update URL on each iteration
                r = requests.get(url)

                # Check if the request was successful
                if r.status_code == 200:
                    # Parse JSON response
                    data = r.json()
                    # Extract links from JSON
                    for job in data.get('jobs', []):
                        full_url = 'https://careerspace.app/job/' + str(job.get('job_id'))

                        # Get salary from JSON
                        salary_currency = job.get('job_salary_currency')
                        сurr_salary_from = сurr_salary_to = salary_from = salary_to = None
                        if 'RUB' in salary_currency:
                            сurr_salary_from = job.get('job_salary_from')
                            сurr_salary_to = job.get('job_salary_to')
                            salary_currency = 'RUR'
                        else:
                            сurr_salary_from = job.get('job_salary_from')
                            сurr_salary_to = job.get('job_salary_to')

                        # Get cities and countries from JSON
                        locations = job.get("locations", {})
                        cities_list = locations.get("cities", [])
                        countries_list = locations.get("countries", [])

                        # Check for cities in JSON before iteration
                        towns = ", ".join(city["name"] for city in cities_list) if isinstance(cities_list, list) else None

                        # Check for countries in JSON before iteration
                        countries = ", ".join(country["name"] for country in countries_list) if isinstance(countries_list, list) else None

                        # Combine cities and countries into a single string separated by commas
                        towns = ", ".join(filter(None, [towns, countries]))

                        # Get job format from JSON
                        formatted_job_format = job.get('job_format')
                        job_format = ", ".join(map(str, formatted_job_format)) if formatted_job_format else None

                        # Go to the job vacancy page and parse the job description
                        resp = requests.get(full_url, headers=HEADERS)
                        vac = BeautifulSoup(resp.content, 'lxml')
                        description = vac.find('div', {'class': 'j-d-desc'}).text.strip()

                        date_created = date_of_download = datetime.now().date()

                        # Set values for experience level
                        if o == 'intern':
                            level = 'Less than 1 year'
                        elif o == 'specialist':
                            level = '1-3 years'
                        elif o == 'manager':
                            level = '3-5 years'
                        elif o == 'head':
                            level = '5-10 years'
                        else:
                            level = 'More than 10 years'

                        item = {
                            "company": job.get('company', {}).get('company_name'),
                            "vacancy_name": job.get('job_name'),
                            "vacancy_url": full_url,
                            "job_format": job_format,
                            "salary_from": salary_from,
                            "salary_to": salary_to,
                            "currency_id": salary_currency,
                            "сurr_salary_from": сurr_salary_from,
                            "сurr_salary_to": сurr_salary_to,
                            "date_created": date_created,
                            "date_of_download": date_of_download,
                            "source_vac": 3,
                            "status": 'existing',
                            "version_vac": '1',
                            "actual": '1',
                            "description": description,
                            "towns": towns,
                            "level": level,
                        }
                        print(f"Adding item: {item}")
                        self.df = pd.concat([self.df, pd.DataFrame(item, index=[0])], ignore_index=True)
                        time.sleep(3)

                    # Check if there is a next page
                    if not data.get('jobs'):
                        break  # If the next page is empty, exit the loop

                    page += 1  # Move to the next page
                else:
                    print(f"Failed to fetch data for {o}. Status code: {r.status_code}")
                    break  # Break the loop on request error
            self.all_items.extend(self.items)

        self.df = self.df.drop_duplicates()
        self.log.info("Total number of found vacancies after removing duplicates: " + str(len(self.df)) + "\n")
