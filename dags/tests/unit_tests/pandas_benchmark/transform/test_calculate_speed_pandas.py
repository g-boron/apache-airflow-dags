""" UNIT TEST """
import pandas as pd
from pytest import approx

from functions.pandas_benchmark.functions.transform import calculate_speed


columns = [
  "id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"
]


def test_empty_frame() -> None:
  sample_data = pd.DataFrame([], columns=columns)
  sample_data["tpep_pickup_datetime"] = pd.to_datetime(
    sample_data["tpep_pickup_datetime"])
  sample_data["tpep_dropoff_datetime"] = pd.to_datetime(
    sample_data["tpep_dropoff_datetime"])
  result_data = calculate_speed(sample_data)

  assert result_data.empty


def test_calculate_speed() -> None:
  sample_data = pd.DataFrame([
    [
      1, pd.to_datetime("2025-12-03 14:00:00"),
      pd.to_datetime("2025-12-03 14:35:00"), 15
    ],
  ], columns=columns)
  result_data = calculate_speed(sample_data)

  assert result_data.shape == (1, 7)
  assert result_data.loc[
           result_data["id"] == 1
           ]["duration"].iloc[0] == pd.Timedelta(seconds=2100)
  assert result_data.loc[
           result_data["id"] == 1
           ]["duration_hours"].iloc[0] == approx(0.583, rel=1e-3)
  assert result_data.loc[
           result_data["id"] == 1
           ]["avg_speed"].iloc[0] == approx(25.71, rel=1e-3)


def test_anomalies() -> None:
  sample_data = pd.DataFrame([
    [
      1, pd.to_datetime("2025-12-03 14:00:00"),
      pd.to_datetime("2025-12-03 14:35:00"), 15
    ],
    [
      2, pd.to_datetime("2025-12-01 17:00:00"),
      pd.to_datetime("2025-12-01 17:50:00"), 0
    ],
    [
      3, pd.to_datetime("2025-12-20 20:00:00"),
      pd.to_datetime("2025-12-20 20:00:01"), 15
    ],
  ], columns=columns)
  result_data = calculate_speed(sample_data)

  assert result_data.shape == (3, 7)
  assert result_data.loc[
           result_data["id"] == 1
           ]["duration"].iloc[0] == pd.Timedelta(seconds=2100)
  assert result_data.loc[
           result_data["id"] == 1
           ]["duration_hours"].iloc[0] == approx(0.583, rel=1e-3)
  assert result_data.loc[
           result_data["id"] == 1
           ]["avg_speed"].iloc[0] == approx(25.71, rel=1e-3)
  assert result_data.loc[
    (result_data["id"].isin([2, 3])) &
    pd.isnull(result_data["avg_speed"])
  ].shape[0] == 2
