"""
### Polars LazyFrame Benchmark DAG

__Version 1.0.0__

The purpose of the DAG is performance benchmark in data transformation.
\n
__Second scenario__ - 3 months of data.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from functions.operational_functions.files.files_cleaner import (
  clean_data_directory
)
from functions.polars_lazyframe_benchmark.functions.transform import (
  transform_main
)
from functions.polars_lazyframe_benchmark.functions.load import insert_into_db
from functions.operational_functions.dags.dags_parameters import (
  get_dag_parameters
)

DAG_NAME = os.path.basename(__file__).split(".")[0]
DAG_PARAMETERS = get_dag_parameters(DAG_NAME)
KWARGS = {
  "data_path": "/opt/airflow/dags/functions/polars_lazyframe_benchmark/data/"
}
DEFAULT_ARGS = {
  "owner": "Grzegorz Boroń"
}

with DAG(
  dag_id=DAG_NAME,
  start_date=datetime.now() - timedelta(days=1),
  schedule=DAG_PARAMETERS["scheduler"],
  description=DAG_PARAMETERS["description"],
  doc_md=__doc__,
  catchup=False,
  default_args=DEFAULT_ARGS,
  tags=["second_scenario"]
) as dag:
  Clean_data_directory = PythonOperator(
    task_id="clean_data_directory",
    python_callable=clean_data_directory,
    op_args=[("daily_report", "payment_report")],
    op_kwargs=KWARGS
  )
  Transform_lazyframe = PythonOperator(
    task_id="Transform_data_lazyframe",
    python_callable=transform_main,
    op_args=["second"],
    op_kwargs=KWARGS
  )
  Load_lazyframe = PythonOperator(
    task_id="load_lazyframe",
    python_callable=insert_into_db,
    op_args=[("daily_report", "payment_report"), "second"],
    op_kwargs=KWARGS
  )
  Clean_data_directory >> Transform_lazyframe >> Load_lazyframe
