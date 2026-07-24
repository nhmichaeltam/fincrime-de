from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta


INCLUDE_DIR = '/usr/local/airflow/include'
DBT_PROJECT_DIR = f'{INCLUDE_DIR}/fincrime_de'

with DAG(
    dag_id='fincrime_daily_transactions',
    description='Daily incremental transaction load followed by dbt transformations',
    start_date=datetime(2026, 7, 1),
    end_date=datetime(2027, 12, 31),
    schedule='@daily',     
    max_active_runs=1,
    catchup=True,
    tags=['fincrime', 'transactions'],
) as dag:

    incremental_load = BashOperator(
        task_id='incremental_load',
        bash_command=f'python {INCLUDE_DIR}/gcs_to_bigquery.py incremental --date {{{{ ds }}}}',
        execution_timeout=timedelta(minutes=45),
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            f'dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}'
        ),
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            f'dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}'
        ),
    )

    incremental_load >> dbt_run >> dbt_test
