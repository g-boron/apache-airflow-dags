"""
### Pandas Benchmark DAG

__Version 2.0.0__

The purpose of the DAG is performance benchmark in data transformation.
\n
__Second scenario__ - 3 months of data.
\n
Changelog:
\n
v2.0.0: Added self-triggering mechanism.
\n
v1.0.0: Added DAG.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator

from functions.operational_functions.files.files_cleaner import (
  clean_data_directory
)
from functions.pandas_benchmark.functions.transform import (
  transform_main
)
from functions.pandas_benchmark.functions.load import insert_into_db
from functions.operational_functions.dags.dags_parameters import (
  get_dag_parameters
)
from functions.operational_functions.dags.self_triggering import (
  init_counter, increment_and_check, stop_trigger
)

DAG_NAME = os.path.basename(__file__).split(".")[0]
DAG_PARAMETERS = get_dag_parameters(DAG_NAME)
KWARGS = {
  "data_path": "/opt/airflow/dags/functions/pandas_benchmark/data/"
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
  with TaskGroup(group_id="Init_trigger") as init_trigger:
    Init = PythonOperator(
      task_id="init_counter",
      python_callable=init_counter,
      op_args=[DAG_NAME]
    )

  with TaskGroup(group_id="ETL") as etl:
    Clean_data_directory = PythonOperator(
      task_id="clean_data_directory",
      python_callable=clean_data_directory,
      op_args=[("daily_report", "payment_report")],
      op_kwargs=KWARGS
    )
    Transform = PythonOperator(
     task_id="transform_data",
     python_callable=transform_main,
     op_args=["second"],
     op_kwargs=KWARGS,
     provide_context=True
    )
    Load = PythonOperator(
      task_id="load",
      python_callable=insert_into_db,
      op_args=[("daily_report", "payment_report"), "second"],
      op_kwargs=KWARGS
    )
    Clean_data_directory >> Transform >> Load

  with TaskGroup(group_id="Check_trigger") as check_trigger:
    Check = BranchPythonOperator(
      task_id="check_and_increment_trigger",
      python_callable=increment_and_check,
      op_args=[DAG_NAME, DAG_PARAMETERS.get("trigger_max_runs")],
    )
    Trigger = TriggerDagRunOperator(
      task_id="trigger_dag",
      trigger_dag_id=DAG_NAME,
      wait_for_completion=False,
      reset_dag_run=True
    )
    Stop = PythonOperator(
      task_id="stop_dag",
      python_callable=stop_trigger,
      op_args=[DAG_NAME]
    )
    Check >> [Trigger, Stop]

  init_trigger >> etl >> check_trigger
