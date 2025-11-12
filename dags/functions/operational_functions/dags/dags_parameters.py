from airflow.models import Variable


def get_dag_parameters(dag_name: str) -> dict:
  """ """
  dags_parameters = Variable.get("dags_parameters", deserialize_json=True)
  dag_parameters = dags_parameters.get(dag_name)
  return {
    "scheduler": dag_parameters.get("scheduler"),
    "description": dags_parameters.get("description")
  }
