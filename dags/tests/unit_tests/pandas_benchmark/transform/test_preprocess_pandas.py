""" UNIT TEST """
from datetime import date

import pandas as pd

from functions.pandas_benchmark.functions.transform import preprocess


columns = [
  "id", "passenger_count", "tpep_pickup_datetime", "tpep_dropoff_datetime"
]


def test_empty_frame() -> None:
  sample_data = pd.DataFrame([], columns=columns)
  sample_data["tpep_pickup_datetime"] = pd.to_datetime(
    sample_data["tpep_pickup_datetime"])
  sample_data["tpep_dropoff_datetime"] = pd.to_datetime(
    sample_data["tpep_dropoff_datetime"])
  result_data = preprocess(sample_data)

  assert result_data.empty


def test_preprocess() -> None:
  sample_data = pd.DataFrame([
    [
      1, 3, pd.to_datetime("2025-12-03 14:00:00"),
      pd.to_datetime("2025-12-03 14:35:00")
    ],
    [
      2, None, pd.to_datetime("2025-12-10 10:00:00"),
      pd.to_datetime("2025-12-10 10:15:00")
    ],
  ], columns=columns)
  result_data = preprocess(sample_data)

  assert result_data.shape == (1, 10)
  assert result_data.loc[result_data["id"] == 2].shape[0] == 0
  assert result_data.loc[
           result_data["id"] == 1
           ]["pickup_date"].iloc[0] == date(2025, 12, 3)
  assert result_data.loc[
           result_data["id"] == 1
           ]["pickup_hour"].iloc[0] == 14
  assert result_data.loc[
           result_data["id"] == 1
           ]["pickup_day"].iloc[0] == "Wednesday"
  assert result_data.loc[
           result_data["id"] == 1
           ]["dropoff_date"].iloc[0] == date(2025, 12, 3)
  assert result_data.loc[
           result_data["id"] == 1
           ]["dropoff_hour"].iloc[0] == 14
  assert result_data.loc[
           result_data["id"] == 1
           ]["dropoff_day"].iloc[0] == "Wednesday"
