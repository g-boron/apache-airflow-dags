""" Loading data into DB """
import os
import logging

import dask.dataframe as dd
from sqlalchemy.types import Date

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
    df = dd.read_parquet(f"{kwargs['data_path']}{file}")
    table_name = f"dask_{scenario}_{file.split('.')[0]}"
    logger.info(f"Loading into {table_name}")
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
    logger.info("Files loaded")
