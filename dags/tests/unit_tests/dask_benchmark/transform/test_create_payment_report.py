""" UNIT TEST """
import pandas as pd
import dask.dataframe as dd
from pytest import approx

from functions.dask_benchmark.functions.transform import create_payment_report


columns = [
  "payment_type", "fare_amount", "extra"
]


def test_empty_frame() -> None:
  df = pd.DataFrame({
    "payment_type": pd.Series(dtype="int32"),
    "fare_amount": pd.Series(dtype="float64"),
    "extra": pd.Series(dtype="float64"),
  })
  sample_data = dd.from_pandas(df, npartitions=1)
  result_data = create_payment_report(sample_data).compute()

  assert result_data.empty


def test_create_payment_report() -> None:
  df = pd.DataFrame([
    [1, 20, 0],
    [2, 10, 4],
    [2, 12, 2],
    [1, 15, 11]
  ], columns=columns)
  sample_data = dd.from_pandas(df, npartitions=1)
  result_data = create_payment_report(sample_data).compute()

  assert result_data.shape == (2, 5)
  assert result_data.loc[result_data["payment_type"] == "Credit card"][
           "fare_amount_sum"].iloc[0] == 35
  assert result_data.loc[result_data["payment_type"] == "Credit card"][
           "fare_amount_median"].iloc[0] == approx(17.5)
  assert result_data.loc[result_data["payment_type"] == "Credit card"][
           "extra_sum"].iloc[0] == 11
  assert result_data.loc[result_data["payment_type"] == "Credit card"][
           "extra_median"].iloc[0] == approx(5.5)
  assert result_data.loc[result_data["payment_type"] == "Cash"][
           "fare_amount_sum"].iloc[0] == 22
  assert result_data.loc[result_data["payment_type"] == "Cash"][
           "fare_amount_median"].iloc[0] == 11
  assert result_data.loc[result_data["payment_type"] == "Cash"][
           "extra_sum"].iloc[0] == 6
  assert result_data.loc[result_data["payment_type"] == "Cash"][
           "extra_median"].iloc[0] == 3
