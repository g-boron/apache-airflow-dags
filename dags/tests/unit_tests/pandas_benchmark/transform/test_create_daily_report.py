""" UNIT TEST """
import pandas as pd
from pytest import approx

from functions.pandas_benchmark.functions.transform import create_daily_report


columns = [
  "pickup_date", "pickup_day", "PULocationID", "DOLocationID",
  "trip_distance", "fare_amount", "tip_amount", "tip_ratio", "passenger_count",
  "VendorID", "avg_speed"
]
location_map_columns = ["LocationID", "Zone"]


def test_empty_frame() -> None:
  sample_data = pd.DataFrame([], columns=columns)
  sample_locations = pd.DataFrame([], columns=location_map_columns)
  result_data = create_daily_report(sample_data, sample_locations)

  assert result_data.empty


def test_create_daily_report() -> None:
  sample_data = pd.DataFrame([
    [
      "2025-12-01", "Sunday", 2, 4, 15, 10, 2, 0.2, 4, "V1", 30
    ],
    [
      "2025-12-01", "Sunday", 2, 4, 8, 8, 1, 0.125, 2, "V2", 20
    ],
    [
      "2025-12-10", "Monday", 1, 3, 45, 20, 10, 0.5, 3, "V3", 60
    ],
    [
      "2025-12-20", "Tuesday", 3, 99, 3, 2, 1, 0.5, 1, "V4", 15
    ]
  ], columns=columns)
  sample_locations = pd.DataFrame([
    [1, "Manhattan"],
    [2, "SoHo"],
    [3, "Greenwich Village"],
    [4, "Brooklyn"]
  ], columns=location_map_columns)
  result_data = create_daily_report(sample_data, sample_locations)

  assert result_data.shape == (3, 12)
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["pickup_location"].iloc[0] == "SoHo"
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["dropoff_location"].iloc[0] == "Brooklyn"
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["mean_trip_distance"].iloc[0] == approx(11.5)
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["mean_fare_amount"].iloc[0] == 9
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["mean_tip_amount"].iloc[0] == approx(1.5)
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["sum_tip_amount"].iloc[0] == 3
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["mean_tip_ratio"].iloc[0] == approx(0.1625)
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["passenger_count"].iloc[0] == 6
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["trips_count"].iloc[0] == 2
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-01"
         ]["mean_speed"].iloc[0] == 25
  assert result_data.loc[
           result_data["pickup_date"] == "2025-12-20"
         ]["dropoff_location"].iloc[0] == "Unknown"
