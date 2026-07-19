from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

INCLUDE_DIR = '/usr/local/airflow/include'

with DAG(
    dag_id='fincrime_reference_load',
    description='One-time load of users and cards reference data into raw layer',
    start_date=datetime(2019, 1, 1),
    schedule=None,
    catchup=False,
    tags=['fincrime', 'reference'],
) as dag:

    load_users = BashOperator(
        task_id='load_users',
        bash_command=f'python {INCLUDE_DIR}/gcs_to_bigquery.py reference --source-file users',
    )

    load_cards = BashOperator(
        task_id='load_cards',
        bash_command=f'python {INCLUDE_DIR}/gcs_to_bigquery.py reference --source-file cards',
    )

    load_users >> load_cards
