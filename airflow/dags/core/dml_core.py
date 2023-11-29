import logging
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)


class DataManager:
    def __init__(self, conn, engine, dict_data_from_model, data_to_closed, descriptions_frame):
        self.conn = conn
        self.cur = conn.cursor()
        self.schema = 'inside_core_schema'
        self.front_schema = 'core_schema'
        self.engine = engine
        self.descriptions = descriptions_frame
        if not data_to_closed.empty:
            self.data_to_closed = pd.DataFrame(data_to_closed['vacancy_url'], columns=['vacancy_url'])
        else:
            self.data_to_closed = pd.DataFrame(columns=['vacancy_url'])
        self.dict_data_from_model = dict_data_from_model
        # self.static_dicts_data = dicts
        self.static_dictionaries_lst = ['job_formats', 'job_types', 'languages',
                                        'sources', 'specialities', 'skills',
                                        'towns', 'experience']
        self.dynamic_dictionaries_lst = ['companies']
        self.link_tables_lst = ['job_formats_vacancies', 'job_types_vacancies', 'languages_vacancies',
                                'specialities_vacancies', 'skills_vacancies', 'towns_vacancies',
                                'experience_vacancies']

    # Type fixing
    def fix_type(self):
        def addapt_numpy_int64(numpy_int64):
            return AsIs(numpy_int64)

        def addapt_numpy_float64(numpy_float64):
            return AsIs(numpy_float64)

        register_adapter(np.int64, addapt_numpy_int64)
        register_adapter(np.float64, addapt_numpy_float64)

    # Load data to DS dictionaries
    def load_to_inside_schema(self, df_name, what_dicts):
        df = what_dicts[df_name]
        if not df.empty:

            self.fix_type()

            # df.to_sql(df_name, self.engine, schema=self.schema, if_exists='append', index=False)
            data_to_load = [tuple(x) for x in df.to_records(index=False)]
            cols = ', '.join(list(df))
            update_query = f"""
            INSERT INTO {self.schema}.{df_name} 
            VALUES ({', '.join(['%s'] * len(list(df)))})
            ON CONFLICT (id) DO UPDATE
            SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in list(df)])})
            """
            self.cur.executemany(update_query, data_to_load)
            logging.info(f'Data loaded into {self.schema}.{df_name} successfully')
        else:
            logging.info(f'No data to loading, dataframe {df_name} is empty')

    # Load data to core dictionaries 
    def load_dictionaries_core(self, df_name, what_data):
        df = what_data[df_name]
        if not df.empty:

            self.fix_type()
            if not df_name == 'experience':
                selected_columns = df[['id', 'title']].copy()
            else:
                selected_columns = df
            data_to_load = [tuple(x) for x in selected_columns.to_records(index=False)]

            # selected_columns.to_sql(str(df_name).replace('', ''), self.engine,
            #                         schema=self.schema, if_exists='append', index=False)

            cols = ', '.join(list(selected_columns))
            update_query = f"""
            INSERT INTO {self.front_schema}.{df_name} 
            VALUES ({', '.join(['%s'] * len(list(selected_columns)))})
            ON CONFLICT (id) DO UPDATE
            SET ({cols}) = ({','.join(['EXCLUDED.' + x for x in list(selected_columns)])})
            """
            self.cur.executemany(update_query, data_to_load)
        else:
            logging.info(f'No data to loading, dataframe {df_name} is empty')

    # Load data to dynamic/static dictionaries (union, commit)
    def load_data_to_dicts(self, dictionaries_lst, what_data):
        self.fix_type()
        logging.info('Loading dictionary tables data')
        for dict_table_name in dictionaries_lst:
            try:
                # Load data to inside tables
                self.load_to_inside_schema(dict_table_name, what_data)
                # Load data to core tables
                self.load_dictionaries_core(dict_table_name, what_data)
                logging.info(f'Data loaded successfully to {dict_table_name}')
                self.conn.commit()
            except Exception as e:
                logging.error(f"Error while data loading to dictionary {dict_table_name}: {e}")
                self.conn.rollback()

    # Init loading and updating links tables    
    def load_data_to_links(self, tables_lst, data):
        logging.info('Loading data to links tables')
        for link_table_name in tables_lst:
            df = data[link_table_name]
            if not df.empty:
                self.fix_type()
                logging.info("Deleting old links")
                # Delete updating vacancies
                delete_updating_data_query = """
                DELETE FROM {0}.{1} WHERE vacancy_id IN %s
                """
                self.cur.execute(delete_updating_data_query.format(self.schema, link_table_name),
                                 (tuple(df['vacancy_id'].tolist()),))
                self.cur.execute(delete_updating_data_query.format(self.front_schema, link_table_name),
                                 (tuple(df['vacancy_id'].tolist()),))
                # Loading data to core
                logging.info("Loading data")
                data_to_load = [tuple(x) for x in df.to_records(index=False)]
                load_data_query = """
                INSERT INTO {0}.{1}
                VALUES ({2})
                """
                self.cur.executemany(load_data_query.format(self.schema, link_table_name,
                                                            ', '.join(['%s'] * len(list(df)))), data_to_load)
                logging.info(f"link {link_table_name} loaded successfully into schema {self.schema}")
                self.cur.executemany(load_data_query.format(self.front_schema, link_table_name,
                                                            ', '.join(['%s'] * len(list(df)))), data_to_load)
                logging.info('Completed')
            else:
                logging.info(f'No data to update {link_table_name}')

    # Max ID update
    def update_tech_table(self):
        clearing_query = f"""
        TRUNCATE TABLE {self.schema}.vacancies_max_id
        """
        self.cur.execute(clearing_query)
        find_max_query = f"""
        WITH union_table AS (
        SELECT MAX(id) AS max_id FROM {self.schema}.vacancies
        UNION ALL
        SELECT MAX(id) AS max_id FROM {self.schema}.archive_vacancies)
        INSERT INTO {self.schema}.vacancies_max_id
        (SELECT MAX(max_id)
        FROM union_table)
        """
        self.cur.execute(find_max_query)

    # Actualize core data (archiving), excluding dictionaries (union, commit)
    def delete_archive_core_data(self):

        self.fix_type()

        # Select actual data
        core_data_load_query = f"""
        SELECT id, url  FROM {self.schema}.vacancies
        """
        core_data = pd.read_sql(core_data_load_query, self.engine)

        # self.data_to_closed = pd.DataFrame({'vacancy_url': ['https://rabota.sber.ru/search/4219605',
        #                                     'https://rabota.sber.ru/search/4221748']})

        old_data = self.data_to_closed.merge(core_data, how='inner', left_on='vacancy_url', right_on='url')
        old_data = old_data.drop('vacancy_url', axis=1)

        if not old_data.empty:

            # Load vacancies' columns list 
            select_vacancy_columns = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{0}' AND table_name = 'archive_vacancies'
            ORDER BY ordinal_position;
            """
            self.cur.execute(select_vacancy_columns.format(self.schema))
            na_vacancies_cols = [row[0] for row in self.cur.fetchall()]

            # Upsert archive vacancies table
            cols = ','.join(na_vacancies_cols)
            load_archive_data_to_na_vacancy = """
            INSERT INTO {0}.archive_vacancies
            SELECT * FROM {0}.vacancies
            WHERE url IN %s
            ON CONFLICT (id) DO UPDATE
            SET ({1}) = ({2})
            """

            urls_tuple = tuple(old_data['url'].tolist())
            ids_tuple = tuple(old_data['id'].tolist())
            # Load data to archive vacancies table
            try:
                # Upsert archive vacancies table
                self.cur.execute(load_archive_data_to_na_vacancy.format(self.schema, cols, ','.join(
                    ['EXCLUDED.' + x for x in na_vacancies_cols])), (urls_tuple,))
                self.cur.execute(load_archive_data_to_na_vacancy.format(self.front_schema, cols, ','.join(
                    ['EXCLUDED.' + x for x in na_vacancies_cols])), (urls_tuple,))

                # Update links tables
                for table_name in self.link_tables_lst:
                    # Delete old links
                    delete_updating_data_query = """
                    DELETE FROM {0}.archive_{1} WHERE vacancy_id IN %s
                    """
                    self.cur.execute(delete_updating_data_query.format(self.schema, table_name), (ids_tuple,))
                    self.cur.execute(delete_updating_data_query.format(self.front_schema, table_name), (ids_tuple,))

                    # Remove old links
                    move_data_query = """
                    INSERT INTO {0}.archive_{1} 
                    SELECT * FROM {0}.{1}
                    WHERE vacancy_id IN %s
                    """

                    # Delete old links from core
                    delete_data_query = """
                    DELETE FROM {0}.{1} WHERE vacancy_id IN %s
                    """
                    self.cur.execute(move_data_query.format(self.schema, table_name), (ids_tuple,))
                    self.cur.execute(delete_data_query.format(self.schema, table_name), (ids_tuple,))

                    self.cur.execute(move_data_query.format(self.front_schema, table_name), (ids_tuple,))
                    self.cur.execute(delete_data_query.format(self.front_schema, table_name), (ids_tuple,))

                # Delete not actual vacancies from core    
                delete_archive = """
                DELETE FROM {0}.{1} WHERE id IN %s;
                """

                self.cur.execute(delete_archive.format(self.schema, 'ds_search'), (ids_tuple,))
                self.cur.execute(delete_archive.format(self.schema, 'vacancy'), (ids_tuple,))

                self.cur.execute(delete_archive.format(self.front_schema, 'ds_search'), (ids_tuple,))
                self.cur.execute(delete_archive.format(self.front_schema, 'vacancy'), (ids_tuple,))
                self.conn.commit()
                logging.info("Archive tables updated successfully")
            except Exception as e:
                logging.error(f"Error: {e}")
                self.conn.rollback()
        else:
            logging.info("No data to remove to archive")
            pass

    # Process. Update data on core-layer (union, commit)
    def load_and_update_actual_data(self):
        logging.info("Loading data starting")
        if not self.dict_data_from_model.get('vacancies').empty:
            try:
                self.fix_type()

                # Loading to dictionaries
                logging.info("Loading data to dictionaries")
                self.load_data_to_dicts(self.dynamic_dictionaries_lst, self.dict_data_from_model)

                logging.info("New data loading to vacancies")

                # Datatype fixing (NaN -> NULL)
                vacancies = self.dict_data_from_model.get('vacancies').fillna(psycopg2.extensions.AsIs('NULL'))

                # Loading data to vacancies
                data_to_load = [tuple(x) for x in vacancies.to_records(index=False)]
                names = list(vacancies)
                cols = ', '.join(names)
                update_query = """
                INSERT INTO {0}.{1} 
                VALUES ({2})
                ON CONFLICT (id) DO UPDATE
                SET ({3}) = ({4});
                """
                self.cur.executemany(update_query.format(self.schema, 'vacancies', ', '.join(['%s'] * len(names)), cols,
                                                         ','.join(['EXCLUDED.' + x for x in names])), data_to_load)
                self.cur.executemany(update_query.format(self.front_schema, 'vacancies', ','.join(['%s'] * len(names)),
                                                         cols, ','.join(['EXCLUDED.' + x for x in names])),
                                     data_to_load)

                logging.info("New data loading to search tables")

                # Load data to ds_search
                data_to_load = [tuple(x) for x in self.dict_data_from_model.get('ds_search').to_records(index=False)]
                names = list(self.dict_data_from_model.get('ds_search'))
                cols = ', '.join(names)
                self.cur.executemany(update_query.format(self.schema, 'ds_search', ', '.join(['%s'] * len(names)), cols,
                                                         ','.join(['EXCLUDED.' + x for x in names])), data_to_load)
                self.cur.executemany(update_query.format(self.front_schema,
                                                         'ds_search', ','.join(['%s'] * len(names)),
                                                         cols, ','.join(['EXCLUDED.' + x for x in names])),
                                     data_to_load)
                logging.info("Data loaded")

                # Load data to links
                logging.info("Loading data to links")
                self.load_data_to_links(self.link_tables_lst, self.dict_data_from_model)

                # Update max id
                self.update_tech_table()
                self.conn.commit()
            except Exception as e:
                logging.error(f'Error while loading data to core tables: {e}')
                self.conn.rollback()
        else:
            logging.info("No data to update")

    # Process. Update vacancies table = pull descriptions (commit)
    def load_descriptions(self):
        try:
            data_to_load = [tuple(x) for x in self.descriptions.to_records(index=False)]
            names = list(self.descriptions)
            cols = ', '.join(names)
            update_query = """
            INSERT INTO {0}.{1} 
            VALUES ({2})
            ON CONFLICT (url) DO UPDATE
            SET ({3}) = ({4});
            """
            self.cur.executemany(update_query.format(self.schema, 'vacancies', ', '.join(['%s'] * len(names)), cols,
                                                     ','.join(['EXCLUDED.' + x for x in names])), data_to_load)
            logging.info(f"Descriptions loaded into {self.schema} successfully")
            self.cur.executemany(update_query.format(self.front_schema, 'vacancies', ','.join(['%s'] * len(names)),
                                                     cols, ','.join(['EXCLUDED.' + x for x in names])),
                                 data_to_load)
            logging.info(f"Descriptions loaded into {self.front_schema} successfully")
        except Exception as e:
            logging.error(f'Error while loading descriptions on core: {e}')

    # Process. Pull or update static dictionaries
    def work_with_static_dicts(self):

        # Reading static dictionaries
        dicts = {
            'job_formats': pd.read_csv("/opt/airflow/dags/core/for_de/dict/job_formats.csv"),
            'languages': pd.read_csv("/opt/airflow/dags/core/for_de/dict/languages.csv"),
            'skills': pd.read_csv("/opt/airflow/dags/core/for_de/dict/skills.csv"),
            'companies': pd.read_csv("/opt/airflow/dags/core/for_de/dict/companies.csv"),
            'job_types': pd.read_csv("/opt/airflow/dags/core/for_de/dict/job_types.csv"),
            'specialities': pd.read_csv("/opt/airflow/dags/core/for_de/dict/specialities.csv"),
            'towns': pd.read_csv("/opt/airflow/dags/core/for_de/dict/towns.csv"),
            'sources': pd.read_csv("/opt/airflow/dags/core/for_de/dict/sources.csv"),
            'experience': pd.read_csv("/opt/airflow/dags/core/for_de/dict/experience.csv")
        }
        only_raw = {
            'control_df': pd.read_csv("/opt/airflow/dags/core/for_de/dict/control_df.csv")
        }
        self.load_to_inside_schema('control_df', only_raw)
        link = {'specialities_skills': pd.read_csv("/opt/airflow/dags/core/for_de/id-id/specialities_skills.csv")}
        try:
            # Loading
            dicts["experience"] = dicts["experience"].fillna(psycopg2.extensions.AsIs('NULL'))
            self.load_data_to_dicts(self.static_dictionaries_lst, dicts)

            self.fix_type()

            data_to_load = [tuple(x) for x in link['specialities_skills'].to_records(index=False)]
            for schema in [self.schema, self.front_schema]:
                delete_old_links = f"""
                TRUNCATE TABLE {schema}.specialities_skills
                """
                self.cur.execute(delete_old_links)
                load_data_query = f"""
                                INSERT INTO {schema}.specialities_skills
                                VALUES ({', '.join(['%s'] * len(list(link['specialities_skills'])))})
                                """
                self.cur.executemany(load_data_query, data_to_load)
            logging.info("Data loaded to static dictionaries successfully")
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error while loading data to static dicts: {e}")
            self.conn.rollback()

    # Process. Init data loading (union, commit)
    def init_load(self):
        try:
            self.load_data_to_dicts(self.dynamic_dictionaries_lst, self.dict_data_from_model)
            self.load_and_update_actual_data()
            self.update_tech_table()
            self.conn.commit()
            self.conn.close()
        except Exception as e:
            self.conn.rollback()
            logging.info(f"Error while init data loading to core: {e}")
            self.conn.close()

    # Process. Update and archive data (union, commited previously)
    def updating(self):
        try:
            self.delete_archive_core_data()
            self.load_and_update_actual_data()
            logging.info("Updated successfully")
            self.conn.close()
        except Exception as e:
            logging.error(f"Error while updating: {e}")
            self.conn.close()
