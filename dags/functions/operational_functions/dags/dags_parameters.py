""" Parsing DAGs configuration. """
from airflow.models import Variable


def get_dag_parameters(dag_name: str) -> dict:
  """
  Parses Airflow variable to get DAGs scheduler settings and description.

  :param dag_name: DAG name set in Airflow variable.
  :return: Dictionary of DAG settings.
  """
  dags_parameters = Variable.get("dags_parameters", deserialize_json=True)
  dag_parameters = dags_parameters.get(dag_name)
  return {
    "scheduler": dag_parameters.get("scheduler"),
    "description": dag_parameters.get("description")
  }
