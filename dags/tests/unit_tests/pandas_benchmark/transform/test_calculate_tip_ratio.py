""" UNIT TEST """
import pandas as pd
from pytest import approx

from functions.pandas_benchmark.functions.transform import calculate_tip_ratio


columns = [
  "id", "tip_amount", "fare_amount"
]


def test_empty_frame() -> None:
  sample_data = pd.DataFrame([], columns=columns)
  result_data = calculate_tip_ratio(sample_data)

  assert result_data.empty


def test_calculate_speed() -> None:
  sample_data = pd.DataFrame([
    [1, 5, 25],
  ], columns=columns)
  result_data = calculate_tip_ratio(sample_data)

  assert result_data.shape == (1, 4)
  assert result_data.loc[
           result_data["id"] == 1
         ]["tip_ratio"].iloc[0] == approx(0.2)


def test_anomalies() -> None:
  sample_data = pd.DataFrame([
    [1, 2, 0.4],
    [2, -5, 10],
    [3, 5, 25],
  ], columns=columns)
  result_data = calculate_tip_ratio(sample_data)

  assert result_data.shape == (3, 4)
  assert result_data.loc[
           result_data["id"] == 3
         ]["tip_ratio"].iloc[0] == approx(0.2)
  assert result_data[
           (result_data["id"].isin([1, 2])) &
           pd.isnull(result_data["tip_ratio"])
         ].shape[0] == 2
