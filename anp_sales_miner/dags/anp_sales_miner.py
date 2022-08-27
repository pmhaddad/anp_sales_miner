from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from anp_sales_miner.helpers.profiler import profile_table
from anp_sales_miner.helpers.validator import validate_table
from anp_sales_miner.jobs.excel_parser import ExcelParser
from anp_sales_miner.jobs.cleaner.fuel_table import FuelTable
from anp_sales_miner.jobs.cleaner.total_table import TotalTable
from anp_sales_miner.jobs.transformer import Transformer

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=20),
    }

dag = DAG(
    'anp_sales_miner',
    default_args=default_args,
    description='A micro-tool to mine data from ANP fuel sales',
    schedule_interval=None,
    start_date=datetime(2022, 5, 26),
    catchup=False,
    tags=['ANP', 'Sales', 'Fuels']
)

pivot_tables = {
    'Tabela dinâmica1': {
        'table_name': 'oil_derivative_fuels',
    },

    'Tabela dinâmica3': {
        'table_name': 'diesel',
    }
}

profiler = PythonOperator(
        task_id='profile_anp_fuel_sales',
        python_callable=profile_table,
        dag=dag
    )

for pivot_name in pivot_tables:
    table_name = pivot_tables[pivot_name]['table_name']

    excel_parser = PythonOperator(
        task_id=f'parse_{table_name}',
        python_callable=ExcelParser(pivot_name, table_name).run,
        dag=dag,
    )

    fuel_table_cleaner = PythonOperator(
        task_id=f'clean_{table_name}',
        python_callable=FuelTable(table_name).run,
        dag=dag
    )

    totals_cleaner = PythonOperator(
        task_id=f'clean_total_{table_name}',
        python_callable=TotalTable(table_name).run,
        dag=dag
    )

    validator = PythonOperator(
        task_id=f'validate_{table_name}',
        python_callable=validate_table,
        op_args=[table_name],
        dag=dag
    )

    transformer = PythonOperator(
        task_id=f'transform_{table_name}',
        python_callable=Transformer(table_name).run,
        dag=dag
    )

    excel_parser >> [fuel_table_cleaner, totals_cleaner] >> validator >> transformer >> profiler
