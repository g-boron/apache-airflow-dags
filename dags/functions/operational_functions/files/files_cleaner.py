""" Cleaning DAGs data directories. """
import os
import shutil
import logging

logger = logging.getLogger("airflow.task")


def clean_data_directory(file_prefixes: tuple, **kwargs) -> None:
  """
  Cleans data directory.

  :param file_prefixes: Files prefixes to clean.
  :param kwargs: Config dictionary.
  """
  files = [
    file for file in os.listdir(kwargs["data_path"])
    if file.startswith(file_prefixes)
  ]
  for file in files:
    try:
      os.remove(f"{kwargs['data_path']}{file}")
    except IsADirectoryError:
      shutil.rmtree(f"{kwargs['data_path']}{file}")
  logger.info("Files removed")
