""" UNIT TEST """
import polars as pl
from pytest import approx

from functions.polars_benchmark.functions.transform import calculate_tip_ratio


columns = [
  "id", "tip_amount", "fare_amount"
]


def test_empty_frame() -> None:
  sample_data = pl.DataFrame([], schema={
    "id": pl.Int32,
    "tip_amount": pl.Float32,
    "fare_amount": pl.Float32,
  })
  result_data = calculate_tip_ratio(sample_data)

  assert result_data.is_empty()


def test_calculate_tip_ratio() -> None:
  sample_data = pl.DataFrame([
    [1, 5, 25],
  ], schema=columns, orient="row")
  result_data = calculate_tip_ratio(sample_data)

  assert result_data.shape == (1, 4)
  assert result_data.filter(
    pl.col("id") == 1
  ).select(pl.col("tip_ratio")).row(0)[0] == approx(0.2)


def test_anomalies() -> None:
  sample_data = pl.DataFrame({
    "id": [1, 2, 3],
    "tip_amount": [2.0, -5.0, 5.0],
    "fare_amount": [0.4, 10.0, 25.0]
  })
  result_data = calculate_tip_ratio(sample_data)

  assert result_data.shape == (3, 4)
  assert result_data.filter(
    pl.col("id") == 3
  ).select(pl.col("tip_ratio")).row(0)[0] == approx(0.2)
  assert result_data.filter(
    (pl.col("id").is_in([1, 2])) &
    (pl.col("tip_ratio").is_null())
  ).shape[0] == 2
