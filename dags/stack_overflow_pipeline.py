from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys, os

# Asegurar path del job
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from jobs import bronze_ingest

default_args= {
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='stack_overflow_pipeline',
    default_args=default_args,
    description='Deployment of a Lakehouse (bronze, silver, gold layers) for Stack Overflow data using Medallion architecture',
    schedule_interval=None,
    catchup=False
) as dag:
    bronze_task = PythonOperator(
        task_id='bronze_ingest',
        python_callable=bronze_ingest.main
    )

bronze_task