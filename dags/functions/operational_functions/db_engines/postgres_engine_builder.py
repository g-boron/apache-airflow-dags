""" Building database connection. """
from airflow.providers.postgres.hooks.postgres import PostgresHook


def build_psql_engine() -> str:
  """
  Returns URI from PSQL connection.

  :return: Connection URI.
  """
  hook = PostgresHook(postgres_conn_id="local_psql")
  return hook.get_uri()
