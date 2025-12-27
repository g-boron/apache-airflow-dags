""" UNIT TEST """
from datetime import datetime, timedelta

import polars as pl
from pytest import approx

from functions.polars_lazyframe_benchmark.functions.transform import (
  calculate_speed
)


columns = [
  "id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"
]


def test_empty_frame() -> None:
  sample_data = pl.LazyFrame([], schema={
    "id": pl.Int32,
    "tpep_pickup_datetime": pl.Datetime,
    "tpep_dropoff_datetime": pl.Datetime,
    "trip_distance": pl.Float32
  })
  result_data = calculate_speed(sample_data).collect()

  assert result_data.is_empty()


def test_calculate_speed() -> None:
  sample_data = pl.LazyFrame([
    [
      1, datetime(2025, 12, 3, 14, 0, 0),
      datetime(2025, 12, 3, 14, 35, 0), 15
    ],
  ], schema=columns, orient="row")
  result_data = calculate_speed(sample_data).collect()

  assert result_data.shape == (1, 7)
  row = result_data.filter(pl.col("id") == 1).row(0, named=True)
  assert row["duration"] == timedelta(seconds=2100)
  assert row["duration_hours"] == approx(0.583, rel=1e-3)
  assert row["avg_speed"] == approx(25.71, rel=1e-3)


def test_anomalies() -> None:
  sample_data = pl.LazyFrame([
    [
      1, datetime(2025, 12, 3, 14, 0, 0),
      datetime(2025, 12, 3, 14, 35, 0), 15
    ],
    [
      2, datetime(2025, 12, 1, 17, 0, 0),
      datetime(2025, 12, 1, 17, 50, 0), 0
    ],
    [
      3, datetime(2025, 12, 20, 20, 0, 0),
      datetime(2025, 12, 20, 20, 0, 1), 15
    ],
  ], schema=columns, orient="row")
  result_data = calculate_speed(sample_data).collect()

  assert result_data.shape == (3, 7)
  row = result_data.filter(pl.col("id") == 1).row(0, named=True)
  assert row["duration"] == timedelta(seconds=2100)
  assert row["duration_hours"] == approx(0.583, rel=1e-3)
  assert row["avg_speed"] == approx(25.71, rel=1e-3)
  assert result_data.filter(
    (pl.col("id").is_in([2, 3])) &
    (pl.col("avg_speed").is_null())
  ).shape[0] == 2
