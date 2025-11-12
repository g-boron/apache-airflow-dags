import os
import shutil


def clean_data_directory(file_prefixes: tuple, **kwargs):
  files = [
    file for file in os.listdir(kwargs["data_path"])
    if file.startswith(file_prefixes)
  ]
  for file in files:
    try:
      os.remove(f"{kwargs['data_path']}{file}")
    except IsADirectoryError:
      shutil.rmtree(f"{kwargs['data_path']}{file}")
  print("Files removed")
