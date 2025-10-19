import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from functions.operational_functions.files.files_cleaner import (
  clean_data_directory
)
from functions.polars_benchmark.functions.transform import (
  transform_main
)
from functions.polars_benchmark.functions.load import insert_into_db


KWARGS = {
  "data_path": "/opt/airflow/dags/functions/polars_benchmark/data/"
}


with DAG(
  dag_id=os.path.basename(__file__),
  start_date=datetime.now() - timedelta(days=1),
  schedule=None
) as dag:
  Clean_data_directory = PythonOperator(
    task_id="clean_data_directory",
    python_callable=clean_data_directory,
    op_args=[("daily_report", "payment_report")],
    op_kwargs=KWARGS
  )
  Transform = PythonOperator(
    task_id="Transform_data",
    python_callable=transform_main,
    op_kwargs=KWARGS
  )
  Load = PythonOperator(
    task_id="load",
    python_callable=insert_into_db,
    op_args=[("daily_report", "payment_report")],
    op_kwargs=KWARGS
  )
  Clean_data_directory >> Transform >> Load
