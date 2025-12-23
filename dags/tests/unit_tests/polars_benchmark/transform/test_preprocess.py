""" UNIT TEST """
from datetime import date, datetime

import polars as pl

from functions.polars_benchmark.functions.transform import preprocess


columns = [
  "id", "passenger_count", "tpep_pickup_datetime", "tpep_dropoff_datetime"
]


def test_empty_frame() -> None:
  sample_data = pl.DataFrame([], schema={
    "id": pl.Int32,
    "passenger_count": pl.Int32,
    "tpep_pickup_datetime": pl.Datetime,
    "tpep_dropoff_datetime": pl.Datetime
  })
  result_data = preprocess(sample_data)

  assert result_data.is_empty()


def test_preprocess() -> None:
  sample_data = pl.DataFrame([
    [
      1, 3, datetime(2025, 12, 3, 14, 0, 0),
      datetime(2025, 12, 3, 14, 35, 0)
    ],
    [
      2, None, datetime(2025, 12, 10, 10, 0, 0),
      datetime(2025, 12, 10, 14, 15, 0)
    ],
  ], schema=columns)
  result_data = preprocess(sample_data)

  assert result_data.shape == (1, 10)
  assert result_data.filter(pl.col("id") == 2).is_empty()
  row = result_data.filter(pl.col("id") == 1).row(0, named=True)
  assert row["pickup_date"] == date(2025, 12, 3)
  assert row["pickup_hour"] == 14
  assert row["pickup_day"] == "Wednesday"
  assert row["dropoff_date"] == date(2025, 12, 3)
  assert row["dropoff_hour"] == 14
  assert row["dropoff_day"] == "Wednesday"
