""" Loading data into DB """
import os
import logging

import polars as pl

from functions.operational_functions.db_engines.postgres_engine_builder import (
  build_psql_engine
)

logger = logging.getLogger("airflow.task")


def insert_into_db(
  file_prefixes: tuple, scenario: str = "first", **kwargs
) -> None:  # pragma: no cover
  """
  Loading transformed data into DB.

  :param file_prefixes: Files prefixes to load.
  :param scenario: Scenario - used for table name.
  :param kwargs: Config dictionary.
  """
  engine = build_psql_engine()
  files = [
    file for file in os.listdir(kwargs["data_path"])
    if file.startswith(file_prefixes)
  ]
  logger.info(f"Files to load: {len(files)}")
  for file in files:
    df = pl.read_parquet(f"{kwargs['data_path']}{file}")
    table_name = f"polars_lazyframe_{scenario}_{file.split('.')[0]}"
    logger.info(f"Loading into {table_name}")
    df.write_database(table_name, engine, if_table_exists="replace")
    logger.info("Files loaded")
