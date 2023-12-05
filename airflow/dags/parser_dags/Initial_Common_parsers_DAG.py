from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from typing import Callable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from raw.currency_directory import exchange_rates
import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import parser_dags.dag_careerspace, parser_dags.dag_getmatch, parser_dags.dag_hh, parser_dags.dag_habrcareer, parser_dags.dag_vseti
import parser_dags.dag_remotejob, parser_dags.dag_sber, parser_dags.dag_tinkoff, parser_dags.dag_vk, parser_dags.dag_yandex

start_date = datetime(2023, 12, 5)

def generate_parser_task(task_id: str, run_parser: Callable, trigger_rule='all_done'):
    """
    Function to generate a PythonOperator for running a parser.
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=run_parser,
        provide_context=True,
        trigger_rule=trigger_rule
    )

def generate_parsing_dag(dag_id: str, task_id: str, run_parser: Callable, start_date):
    """
    Function to generate a DAG for parsing tasks.
    """
    dag = DAG(
        dag_id=dag_id,
        default_args={
            "owner": "admin_1T",
            'retry_delay': timedelta(minutes=5),
        },
        start_date=start_date,
        # schedule_interval='@daily',
        schedule_interval=None,
    )

    with dag:
        hello_bash_task = BashOperator(
            task_id='hello_task',
            bash_command='echo "Wishing you successful parsing! May the reliable internet be with us!"'
        )

        parsed_task = generate_parser_task(task_id=task_id, run_parser=run_parser, trigger_rule='all_done')

        end_task = DummyOperator(
            task_id="end_task"
        )

        hello_bash_task >> parsed_task >> end_task

    return dag


initial_common_dag_id = 'INITIAL_COMMON_PARSING_TASK'

with DAG(
    dag_id=initial_common_dag_id,
    default_args={
        "owner": "admin_1T",
        'retry_delay': timedelta(minutes=5),
    },
    start_date=start_date,
    schedule_interval=None,
    ) as initial_common_dag:

    with TaskGroup('initial_parsers') as parsers:

        hello_bash_task = BashOperator(
            task_id='hello_task',
            bash_command='echo "Wishing you successful parsing! May the reliable internet be with us!"',
        )

        init_currency_task = PythonOperator(
            task_id='init_currency_task',
            python_callable=exchange_rates,
            trigger_rule='all_done',
        )

        end_task = DummyOperator(
            task_id="end_task",
        )


        with TaskGroup('parsers_group') as parsers_group:

            init_careerspace_task = generate_parser_task('init_careerspace_task',
                                                         parser_dags.dag_careerspace.init_call_all_func,
                                                         trigger_rule='all_done')
            init_getmatch_task = generate_parser_task('init_getmatch_task',
                                                         parser_dags.dag_getmatch.init_call_all_func,
                                                         trigger_rule='all_done')
            init_habrcareer_task = generate_parser_task('init_habrcareer_task',
                                                         parser_dags.dag_habrcareer.init_call_all_func,
                                                         trigger_rule='all_done')
            init_headhunter_task = generate_parser_task('init_headhunter_task',
                                                         parser_dags.dag_hh.init_call_all_func,
                                                         trigger_rule='all_done')
            init_vseti_task = generate_parser_task('init_vseti_task',
                                                         parser_dags.dag_vseti.init_call_all_func,
                                                         trigger_rule='all_done')
            init_vkjob_task = generate_parser_task('init_vkjob_task',
                                                         parser_dags.dag_vk.init_call_all_func,
                                                         trigger_rule='all_done')
            init_sber_task = generate_parser_task('init_sber_task',
                                                         parser_dags.dag_sber.init_call_all_func,
                                                         trigger_rule='all_done')
            init_tinkoff_task = generate_parser_task('init_tinkoff_task',
                                                         parser_dags.dag_tinkoff.init_call_all_func,
                                                         trigger_rule='all_done')
            init_yandex_task = generate_parser_task('init_yandex_task',
                                                         parser_dags.dag_yandex.init_call_all_func,
                                                         trigger_rule='all_done')
            init_remotejob_task = generate_parser_task('init_remotejob_task',
                                                         parser_dags.dag_remotejob.init_call_all_func,
                                                         trigger_rule='all_done')

            # Define the execution order of tasks within the task group
            init_careerspace_task >> init_getmatch_task >> init_habrcareer_task >> \
            init_headhunter_task >> init_vseti_task >> init_vkjob_task >> init_sber_task >> init_tinkoff_task \
            >> init_yandex_task >> init_remotejob_task

        hello_bash_task >> init_currency_task >> parsers_group >> end_task

# Create separate DAGs for each parsing task

init_careerspace_dag = generate_parsing_dag('init_careerspace_dag', 'initial_careerspace',
                                      parser_dags.dag_careerspace.init_call_all_func, start_date)
init_getmatch_dag = generate_parsing_dag('init_getmatch_dag', 'initial_getmatch',
                                      parser_dags.dag_getmatch.init_call_all_func, start_date)
init_habrcareer_dag = generate_parsing_dag('init_habrcareer_dag', 'initial_habrcareer',
                                      parser_dags.dag_habrcareer.init_call_all_func, start_date)
init_headhunter_dag = generate_parsing_dag('init_headhunter_dag', 'initial_headhunter',
                                      parser_dags.dag_hh.init_call_all_func, start_date)
init_vseti_dag = generate_parsing_dag('init_vseti_dag', 'initial_vseti',
                                      parser_dags.dag_vseti.init_call_all_func, start_date)
init_vk_dag = generate_parsing_dag('init_vk_dag', 'initial_vk',
                                      parser_dags.dag_vk.init_call_all_func, start_date)
init_sber_dag = generate_parsing_dag('init_sber_dag', 'initial_sber',
                                      parser_dags.dag_sber.init_call_all_func, start_date)
init_tinkoff_dag = generate_parsing_dag('init_tinkoff_dag', 'initial_tinkoff',
                                      parser_dags.dag_tinkoff.init_call_all_func, start_date)
init_yandex_dag = generate_parsing_dag('init_yandex_dag', 'initial_yandex',
                                      parser_dags.dag_yandex.init_call_all_func, start_date)
init_remotejob_dag = generate_parsing_dag('init_remotejob_dag', 'initial_remotejob',
                                      parser_dags.dag_remotejob.init_call_all_func, start_date)

# Make DAGs globally accessible
globals()[initial_common_dag_id] = initial_common_dag
globals()[init_careerspace_dag.dag_id] = init_careerspace_dag
globals()[init_getmatch_dag.dag_id] = init_getmatch_dag
globals()[init_habrcareer_dag.dag_id] = init_habrcareer_dag
globals()[init_headhunter_dag.dag_id] = init_headhunter_dag
globals()[init_vseti_dag.dag_id] = init_vseti_dag
globals()[init_vk_dag.dag_id] = init_vk_dag
globals()[init_sber_dag.dag_id] = init_sber_dag
globals()[init_tinkoff_dag.dag_id] = init_tinkoff_dag
globals()[init_yandex_dag.dag_id] = init_yandex_dag
globals()[init_remotejob_dag.dag_id] = init_remotejob_dag

