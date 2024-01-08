from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import rtl as rt
import extractions as ex
import integration as int
import dashboards as db

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

dag = DAG(
    'green_taxis_01_2015_pipeline',
    default_args=default_args,
    description='green taxis pipeline'
)
with DAG(
    dag_id='green_taxis_01_2015_pipeline',
    schedule_interval='@once',
    default_args=default_args,
    tags=['green_taxis_pipeline']
)as dag:
    rtl_task = PythonOperator(
        task_id='rtl_task',
        python_callable=rt.rtl_task,
        op_kwargs={
            "filename": '/opt/airflow/data/green_tripdata_2015-01.csv'
        }
    )
    extraction_task = PythonOperator(
        task_id='extraction_task',
        python_callable=ex.extraction_task,
        op_kwargs={
            "filename": '/opt/airflow/data/green_trip_data_2015-1_before_encoding.csv'
        }
    )
    integration_task = PythonOperator(
        task_id='integration_task',
        python_callable=int.integration_task,
        op_kwargs={
            "filename_rtl": '/opt/airflow/data/green_trip_data_2015-1_rtl.csv',
            "filename_gps": '/opt/airflow/data/green_trip_data_2015-1_with_gps.csv',
            "filename_lookup": '/opt/airflow/data/lookup_table_green_taxis.csv'
        }
    )
    dashboards_task = PythonOperator(
        task_id='dashboards_task',
        python_callable=db.initialize_dashboards,
        op_kwargs={
            "filename": '/opt/airflow/data/green_trip_data_2015-1_before_encoding.csv'
        }
    )

    rtl_task >> extraction_task >> integration_task >> dashboards_task