""" UNIT TEST """
import pandas as pd
import dask.dataframe as dd
from pytest import approx

from functions.dask_benchmark.functions.transform import calculate_tip_ratio


columns = [
  "id", "tip_amount", "fare_amount"
]


def test_empty_frame() -> None:
  df = pd.DataFrame({
    "id": pd.Series(dtype="int64"),
    "tip_amount": pd.Series(dtype="float64"),
    "fare_amount": pd.Series(dtype="float64"),
  })
  sample_data = dd.from_pandas(df, npartitions=1)
  result_data = calculate_tip_ratio(sample_data).compute()

  assert result_data.empty


def test_calculate_tip_ratio() -> None:
  df = pd.DataFrame([
    [1, 5, 25],
  ], columns=columns)
  sample_data = dd.from_pandas(df, npartitions=1)
  result_data = calculate_tip_ratio(sample_data).compute()

  assert result_data.shape == (1, 4)
  assert result_data.loc[
           result_data["id"] == 1
         ]["tip_ratio"].iloc[0] == approx(0.2)


def test_anomalies() -> None:
  df = pd.DataFrame([
    [1, 2, 0.4],
    [2, -5, 10],
    [3, 5, 25],
  ], columns=columns)
  sample_data = dd.from_pandas(df, npartitions=1)
  result_data = calculate_tip_ratio(sample_data).compute()

  assert result_data.shape == (3, 4)
  assert result_data.loc[
           result_data["id"] == 3
         ]["tip_ratio"].iloc[0] == approx(0.2)
  assert result_data[
           (result_data["id"].isin([1, 2])) &
           pd.isnull(result_data["tip_ratio"])
         ].shape[0] == 2
