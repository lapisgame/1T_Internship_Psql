import logging
from raw.connect_settings import conn, engine
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# logging configuration parameters
logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)


class DatabaseManager:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.schema = 'inside_core_schema'
        self.front_schema = 'core_schema'

    def create_schema(self):
        try:
            create_schema_query = "CREATE SCHEMA IF NOT EXISTS {0}"
            self.cur.execute(create_schema_query.format(self.schema))
            logging.info(f"{self.schema} created")
            self.cur.execute(create_schema_query.format(self.front_schema))
            logging.info(f"{self.front_schema} created")
            self.conn.commit()
        except Exception as e:
            logging.error(f'Schemas creating failed with error: {e}')
            self.conn.rollback()

    def create_tech_table(self):
        try:
            tech_schema_query = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.vacancies_max_id(
            max_id INT
            );
            """
            self.cur.execute(tech_schema_query)
            self.conn.commit()

            self.cur.execute(f"INSERT INTO {self.schema}.vacancies_max_id(max_id) VALUES (0);")
            self.conn.commit()
            logging.info(f"Max id table created successfully in {self.schema}")
        except Exception as e:
            logging.error(f"Max id table creating failed with error: {e}")
            self.conn.rollback()

    def create_inside_dictionaries(self):
        try:
            create_raw_dict_table_query = """                
            CREATE TABLE IF NOT EXISTS {0}.job_formats(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.languages(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.skills(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.job_types(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.specialities(
            id SERIAL PRIMARY KEY,
            title VARCHAR(100), 
            tag VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS {0}.towns(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50), 
            clear_title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.sources(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.companies(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );
            
            CREATE TABLE IF NOT EXISTS {0}.experience(
            id SERIAL PRIMARY KEY,
            exp_from DECIMAL(2,1),
            exp_to DECIMAL(2,1)
            );
            """
            self.cur.execute(create_raw_dict_table_query.format(self.schema))
            self.conn.commit()
            logging.info(f'Dictionary tables created successfully in schema {self.schema}')
        except Exception as e:
            logging.error(f'Dictionary tables creating in schema {self.schema} failed with error": {e}')
            self.conn.rollback()

    def create_core_dictionaries(self):
        try:
            create_dict_table_query = """                
            CREATE TABLE IF NOT EXISTS {0}.job_formats(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.languages(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.skills(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.job_types(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.specialities(
            id SERIAL PRIMARY KEY,
            title VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS {0}.towns(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.sources(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS {0}.companies(
            id SERIAL PRIMARY KEY,
            title VARCHAR(50)
            );
            
            CREATE TABLE IF NOT EXISTS {0}.experience(
            id SERIAL PRIMARY KEY,
            exp_from DECIMAL(2,1),
            exp_to DECIMAL(2,1)
            );
            """
            self.cur.execute(create_dict_table_query.format(self.front_schema))
            self.conn.commit()
            logging.info(f'Dictionary tables created successfully in schema {self.front_schema}')
        except Exception as e:
            logging.error(f'Dictionary tables creating in schema {self.front_schema} failed with error": {e}')
            self.conn.rollback()

    def create_vacancies_table(self):
        create_vacancy_table_query = """ 
        CREATE TABLE IF NOT EXISTS {0}.vacancies(
        id BIGSERIAL PRIMARY KEY,
        "version" INT NOT NULL,
        "url" VARCHAR(2073) NOT NULL,
        title VARCHAR(255),
        salary_from INT,
        salary_to INT,
        experience_from SMALLINT,
        experience_to SMALLINT,
        "description" TEXT,
        company_id INT,
        FOREIGN KEY (company_id) REFERENCES {0}.companies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        source_id INT,
        FOREIGN KEY (source_id) REFERENCES {0}.sources (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        publicated_at DATE
        );
        """
        try:
            self.cur.execute(create_vacancy_table_query.format(self.front_schema))
            logging.info(f'Vacancies table created successfully in {self.front_schema}')
            self.conn.commit()
        except Exception as e:
            logging.error(f'Vacancies table creating in {self.front_schema} failed with error: {e}')
            self.conn.rollback()
        try:
            self.cur.execute(create_vacancy_table_query.format(self.schema))
            logging.info(f'Vacancies table created successfully in {self.schema}')
            self.conn.commit()
        except Exception as e:
            logging.error(f'Vacancies table creating in {self.schema} failed with error: {e}')
            self.conn.rollback()

    def create_archive_vacancies_table(self):
        create_archive_vacancy_table_query = """ 
        CREATE TABLE IF NOT EXISTS {0}.archive_vacancies(
        id BIGSERIAL PRIMARY KEY,
        "version" INT NOT NULL,
        "url" VARCHAR(2073) NOT NULL,
        title VARCHAR(255),
        salary_from INT,
        salary_to INT,
        experience_from SMALLINT,
        experience_to SMALLINT,
        "description" TEXT,
        company_id INT,
        FOREIGN KEY (company_id) REFERENCES {0}.companies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        source_id INT,
        FOREIGN KEY (source_id) REFERENCES {0}.sources (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        publicated_at DATE
        );
        """
        try:
            self.cur.execute(create_archive_vacancy_table_query.format(self.schema))
            self.conn.commit()
            logging.info(f'Archive vacancies table created successfully in {self.schema}')
        except Exception as e:
            logging.error(f'Archive vacancies table creating in {self.schema} failed with error: {e}')
            self.conn.rollback()
        try:
            self.cur.execute(create_archive_vacancy_table_query.format(self.front_schema))
            self.conn.commit()
            logging.info(f'Archive vacancies table created successfully in {self.front_schema}')
        except Exception as e:
            logging.error(f'Archive vacancies table creating in {self.front_schema} failed with error: {e}')
            self.conn.rollback()

    def create_link_tables(self):
        create_link_tables_query = """ 
        CREATE TABLE IF NOT EXISTS {0}.job_formats_vacancies (
        vacancy_id BIGINT,
        job_format_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (job_format_id) REFERENCES {0}.job_formats (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.languages_vacancies (
        vacancy_id BIGINT,
        language_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (language_id) REFERENCES {0}.languages (id) ON UPDATE CASCADE ON DELETE RESTRICT	
        );

        CREATE TABLE IF NOT EXISTS {0}.skills_vacancies (
        vacancy_id BIGINT,
        skill_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (skill_id) REFERENCES {0}.skills (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.job_types_vacancies (
        vacancy_id BIGINT,
        job_type_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (job_type_id) REFERENCES {0}.job_types (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.specialities_vacancies (
        vacancy_id BIGINT,
        spec_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (spec_id) REFERENCES {0}.specialities (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.towns_vacancies (
        vacancy_id BIGINT,
        town_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (town_id) REFERENCES {0}.towns (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.ds_search(
        id BIGINT,
        vector TEXT,
        FOREIGN KEY (id) REFERENCES {0}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );
        
        CREATE TABLE IF NOT EXISTS {0}.experience_vacancy(
        vacancy_id BIGINT,
        experience_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (experience_id) REFERENCES {0}.experience (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );
        
        CREATE TABLE IF NOT EXISTS {0}.specialities_skills(
        spec_id BIGINT,
        skill_id INT,
        FOREIGN KEY (spec_id) REFERENCES {0}.specialities (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (skill_id) REFERENCES {0}.skills (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );
        """
        try:
            self.cur.execute(create_link_tables_query.format(self.schema))
            logging.info(f'Link tables creating in schema {self.schema} completed successfully')
            self.cur.execute(create_link_tables_query.format(self.front_schema))
            logging.info(f'Link tables creating in schema {self.front_schema} completed successfully')
            self.conn.commit()
        except Exception as e:
            logging.error(f'Error while link tables creating: {e}')
            self.conn.rollback()
            
    def create_archive_link_tables(self):
        create_archive_link_tables_query = """ 
        CREATE TABLE IF NOT EXISTS {0}.archive_job_formats_vacancies (
        vacancy_id BIGINT,
        job_format_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.archive_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (job_format_id) REFERENCES {0}.job_formats (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.archive_languages_vacancies (
        vacancy_id BIGINT,
        language_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.archive_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (language_id) REFERENCES {0}.languages (id) ON UPDATE CASCADE ON DELETE RESTRICT	
        );

        CREATE TABLE IF NOT EXISTS {0}.archive_skills_vacancies (
        vacancy_id BIGINT,
        skill_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.archive_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (skill_id) REFERENCES {0}.skills (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.archive_job_types_vacancies (
        vacancy_id BIGINT,
        job_type_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.archive_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (job_type_id) REFERENCES {0}.job_types (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.archive_specialities_vacancies (
        vacancy_id BIGINT,
        spec_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.archive_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (spec_id) REFERENCES {0}.specialities (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );
        
        CREATE TABLE IF NOT EXISTS {0}.archive_experience_vacancy(
        vacancy_id BIGINT,
        experience_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.archive_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (experience_id) REFERENCES {0}.experience (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS {0}.archive_towns_vacancies (
        vacancy_id BIGINT,
        town_id INT,
        FOREIGN KEY (vacancy_id) REFERENCES {0}.archive_vacancies (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (town_id) REFERENCES {0}.towns (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );
        
        CREATE TABLE IF NOT EXISTS {0}.archive_specialities_skills(
        spec_id BIGINT,
        skill_id INT,
        FOREIGN KEY (spec_id) REFERENCES {0}.specialities (id) ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (skill_id) REFERENCES {0}.skills (id) ON UPDATE CASCADE ON DELETE RESTRICT
        );
        """

        try:
            self.cur.execute(create_archive_link_tables_query.format(self.schema))
            logging.info(f'Link tables creating in schema {self.schema} completed successfully')
            self.cur.execute(create_archive_link_tables_query.format(self.front_schema))
            logging.info(f'Link tables creating in schema {self.front_schema} completed successfully')
            self.conn.commit()
        except Exception as e:
            logging.error(f'Error while link tables creating: {e}')
            self.conn.rollback()

    def db_creator(self):
        try:
            logging.info('Creating core layer started')
            self.create_schema()
            self.create_tech_table()
            self.create_core_dictionaries()
            self.create_inside_dictionaries()
            self.create_vacancies_table()
            self.create_archive_vacancies_table()
            self.create_link_tables()
            self.create_archive_link_tables()
            logging.info('Successfully')
        except Exception as e:
            logging.error(f'Error while core layer creating: {e}')
