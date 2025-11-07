import os

import dask.dataframe as dd
from sqlalchemy.types import Date

from functions.operational_functions.db_engines.postgres_engine_builder import (
  build_psql_engine
)


def insert_into_db(file_prefixes: tuple, **kwargs) -> None:
  """ """
  engine = build_psql_engine()
  files = [
    file for file in os.listdir(kwargs["data_path"])
    if file.startswith(file_prefixes)
  ]
  print(f"Files to load: {len(files)}")
  for file in files:
    df = dd.read_parquet(f"{kwargs['data_path']}{file}")
    table_name = f"dask_{file.split('.')[0]}"
    print(f"Loading into {table_name}")
    if "daily_report" in table_name:
      df.to_sql(
        table_name, engine, index=False,
        if_exists="replace", dtype={"pickup_date": Date()}
      )
    else:
      df.to_sql(
        table_name, engine, index=False,
        if_exists="replace"
      )
    print("Files loaded")
