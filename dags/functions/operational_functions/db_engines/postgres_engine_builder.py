from airflow.providers.postgres.hooks.postgres import PostgresHook


def build_psql_engine():
  hook = PostgresHook(postgres_conn_id="local_psql")
  return hook.get_uri()
