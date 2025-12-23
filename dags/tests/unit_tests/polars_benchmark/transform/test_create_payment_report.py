""" UNIT TEST """
import polars as pl
from pytest import approx

from functions.polars_benchmark.functions.transform import create_payment_report


columns = [
  "payment_type", "fare_amount", "extra"
]


def test_empty_frame() -> None:
  sample_data = pl.DataFrame([], schema=columns)
  result_data = create_payment_report(sample_data)

  assert result_data.is_empty()


def test_create_payment_report() -> None:
  sample_data = pl.DataFrame([
    [1, 20, 0],
    [2, 10, 4],
    [2, 12, 2],
    [1, 15, 11]
  ], schema=columns)
  result_data = create_payment_report(sample_data)

  assert result_data.shape == (2, 5)
  row = result_data.filter(pl.col("payment_type") == "Credit card").row(
    0, named=True)
  assert row["fare_amount_sum"] == 35
  assert row["fare_amount_median"] == approx(17.5)
  assert row["extra_sum"] == 11
  assert row["extra_median"] == approx(5.5)
  row = result_data.filter(pl.col("payment_type") == "Cash").row(
    0, named=True)
  assert row["fare_amount_sum"] == 22
  assert row["fare_amount_median"] == 11
  assert row["extra_sum"] == 6
  assert row["extra_median"] == 3
