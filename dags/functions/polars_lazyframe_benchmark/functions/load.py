import os

import polars as pl

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
    df = pl.read_parquet(f"{kwargs['data_path']}{file}")
    table_name = f"polars_{file.split('.')[0]}"
    print(f"Loading into {table_name}")
    df.write_database(table_name, engine, if_table_exists="replace")
    print("Files loaded")
