"""
### Dask Benchmark DAG

__Version 1.0.0__

The purpose of the DAG is performance benchmark in data transformation.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from functions.operational_functions.files.files_cleaner import (
  clean_data_directory
)
from functions.dask_benchmark.functions.transform import (
  transform_main
)
from functions.dask_benchmark.functions.load import insert_into_db
from functions.operational_functions.dags.dags_parameters import (
  get_dag_parameters
)

DAG_NAME = os.path.basename(__file__).split(".")[0]
DAG_PARAMETERS = get_dag_parameters(DAG_NAME)
KWARGS = {
  "data_path": "/opt/airflow/dags/functions/dask_benchmark/data/"
}


with DAG(
  dag_id=DAG_NAME,
  start_date=datetime.now() - timedelta(days=1),
  schedule=DAG_PARAMETERS["scheduler"],
  description=DAG_PARAMETERS["description"],
  doc_md=__doc__
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
