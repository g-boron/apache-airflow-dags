import os

import pandas as pd

from functions.operational_functions.db_engines.postgres_engine_builder import (
  build_psql_engine
)


def insert_into_db(file_prefixes: tuple, **kwargs):
  engine = build_psql_engine()
  files = [
    file for file in os.listdir(kwargs["data_path"])
    if file.startswith(file_prefixes)
  ]
  print(f"Files to load: {len(files)}")
  for file in files:
    df = pd.read_parquet(f"{kwargs['data_path']}{file}")
    table_name = f"pandas_{file.split('.')[0]}"
    print(f"Loading into {table_name}")
    df.to_sql(
      table_name, engine, index=False,
      if_exists="replace"
    )
    print("Files loaded")
