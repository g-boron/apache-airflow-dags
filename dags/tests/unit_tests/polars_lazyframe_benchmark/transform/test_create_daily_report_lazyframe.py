""" UNIT TEST """
from datetime import date

import polars as pl
from pytest import approx

from functions.polars_lazyframe_benchmark.functions.transform import (
  create_daily_report
)


columns = [
  "pickup_date", "pickup_day", "PULocationID", "DOLocationID",
  "trip_distance", "fare_amount", "tip_amount", "tip_ratio", "passenger_count",
  "VendorID", "avg_speed"
]
location_map_columns = ["LocationID", "Zone"]


def test_empty_frame() -> None:
  sample_data = pl.LazyFrame([], schema=columns)
  sample_locations = pl.LazyFrame([], schema=location_map_columns)
  sample_locations = sample_locations.with_columns(
    pl.col("LocationID").cast(pl.Int64)
  )
  result_data = create_daily_report(sample_data, sample_locations)

  assert result_data.is_empty()


def test_create_daily_report() -> None:
  sample_data = pl.LazyFrame([
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
  ], schema=columns, orient="row")
  sample_locations = pl.LazyFrame([
    [1, "Manhattan"],
    [2, "SoHo"],
    [3, "Greenwich Village"],
    [4, "Brooklyn"]
  ], schema=location_map_columns, orient="row")
  result_data = create_daily_report(sample_data, sample_locations)

  assert result_data.shape == (3, 12)
  row = result_data.filter(pl.col("pickup_date") == date(
    2025, 12, 1)).row(0, named=True)
  assert row["pickup_location"] == "SoHo"
  assert row["dropoff_location"] == "Brooklyn"
  assert row["mean_trip_distance"] == approx(11.5)
  assert row["mean_fare_amount"] == 9
  assert row["mean_tip_amount"] == approx(1.5)
  assert row["sum_tip_amount"] == 3
  assert row["mean_tip_ratio"] == approx(0.1625)
  assert row["passenger_count"] == 6
  assert row["trips_count"] == 2
  assert row["mean_speed"] == 25
  assert result_data.filter(pl.col("pickup_date") == date(2025, 12, 20)).select(
    pl.col("dropoff_location")
  ).row(0)[0] == "Unknown"
