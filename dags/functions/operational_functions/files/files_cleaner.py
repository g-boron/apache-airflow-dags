import os


def clean_data_directory(file_prefixes: tuple, **kwargs):
  files = [
    file for file in os.listdir(kwargs["data_path"])
    if file.startswith(file_prefixes)
  ]
  for file in files:
    os.remove(f"{kwargs['data_path']}{file}")
  print("Files removed")
