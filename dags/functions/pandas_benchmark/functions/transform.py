""" Transforming data """
from time import perf_counter
import logging

import pandas as pd

from functions.operational_functions.metrics.resources import (
  measure_resources
)

logger = logging.getLogger("airflow.task")


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
  """
  Preprocess data - drops empty passenger count and creates date related
  columns.

  :param df: Input DataFrame.
  :return: Preprocessed DataFrame.
  """
  start_preprocess = perf_counter()
  logger.info(f"DF shape before dropna: {df.shape}")
  df = df.dropna(subset=["passenger_count"]).copy()
  logger.info(f"DF shape after dropna: {df.shape}")
  df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
  df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
  df["pickup_day"] = df["tpep_pickup_datetime"].dt.day_name()
  df["dropoff_date"] = df["tpep_dropoff_datetime"].dt.date
  df["dropoff_hour"] = df["tpep_dropoff_datetime"].dt.hour
  df["dropoff_day"] = df["tpep_dropoff_datetime"].dt.day_name()
  end_preprocess = perf_counter()
  logger.info(f"Preprocess time: {end_preprocess - start_preprocess}")
  return df


def calculate_speed(df: pd.DataFrame) -> pd.DataFrame:
  """
  Calculates taxi speed in mph. Removes anomalies - average speed more than
  100 mph, trip distance equals to 0 miles or trip duration less than 1 minute.

  :param df: Input DataFrame.
  :return: Dataframe with calculated taxi speed.
  """
  start_calculate = perf_counter()
  df["duration"] = df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
  df.loc[df["duration"] <= pd.Timedelta(value=0), "duration"] = None
  df["duration_hours"] = df["duration"].dt.total_seconds() / 3600
  df["avg_speed"] = df["trip_distance"] / df["duration_hours"]
  df.loc[
    (df["avg_speed"] > 100) |
    (df["duration"] < pd.Timedelta(value=60, unit="s")) |
    (df["trip_distance"] == 0), "avg_speed"
  ] = None
  end_calculate = perf_counter()
  logger.info(f"Calculating speed time: {end_calculate - start_calculate}")
  return df


def calculate_tip_ratio(df: pd.DataFrame) -> pd.DataFrame:
  """
  Calculates tip ratio by dividing tip amount by fare amount. Removes
  anomalies - tip ratio less than 0, fare amount less than 1 USD and tip ratio
  more than 100.

  :param df: Input DataFrame.
  :return: DataFrame with calculated tip ratio.
  """
  start_calculate = perf_counter()
  df["tip_ratio"] = df["tip_amount"] / df["fare_amount"]
  df.loc[
    (df["tip_ratio"] < 0) |
    (df["fare_amount"] < 1) |
    (df["tip_ratio"] > 100), "tip_ratio"
  ] = None
  end_calculate = perf_counter()
  logger.info(f"Calculating tip ratio time: {end_calculate - start_calculate}")
  return df


def create_daily_report(
  df: pd.DataFrame, location_map: pd.DataFrame
) -> pd.DataFrame:
  """
  Creates daily report - data grouped by pickup date, pickup day, departure
  location and arrival location and aggregates mean trip distance, fare amount,
  tip amount, tip ratio and average speed, sum of tip amount and passenger
  count and count of trips.

  :param df: Input DataFrame.
  :param location_map: Locations DataFrame map.
  :return: Daily report.
  """
  start_create = perf_counter()
  daily_report = df.groupby([
    "pickup_date", "pickup_day", "PULocationID", "DOLocationID"
  ]).agg({
    "trip_distance": "mean",
    "fare_amount": "mean",
    "tip_amount": ["mean", "sum"],
    "tip_ratio": "mean",
    "passenger_count": "sum",
    "VendorID": "count",
    "avg_speed": "mean"
  }).reset_index()
  daily_report.columns = [
    "pickup_date", "pickup_day", "pickup_location", "dropoff_location",
    "mean_trip_distance", "mean_fare_amount", "mean_tip_amount",
    "sum_tip_amount", "mean_tip_ratio", "passenger_count", "trips_count",
    "mean_speed"
  ]
  daily_report = daily_report.merge(
    location_map, left_on="pickup_location", right_on="LocationID", how="left"
  ).drop(["pickup_location", "LocationID"], axis=1).rename(
    {"Zone": "pickup_location"}, axis=1
  )
  daily_report = daily_report.merge(
    location_map, left_on="dropoff_location", right_on="LocationID", how="left"
  ).drop(["dropoff_location", "LocationID"], axis=1).rename(
    {"Zone": "dropoff_location"}, axis=1
  )
  daily_report["pickup_location"] = daily_report["pickup_location"].fillna(
    "Unknown")
  daily_report["dropoff_location"] = daily_report["dropoff_location"].fillna(
    "Unknown")
  daily_report = daily_report[[
    "pickup_date", "pickup_day", "pickup_location", "dropoff_location",
    "mean_trip_distance", "mean_fare_amount", "mean_tip_amount",
    "sum_tip_amount", "mean_tip_ratio", "passenger_count", "trips_count",
    "mean_speed"
  ]]
  end_create = perf_counter()
  logger.info(f"Creating daily report time: {end_create - start_create}")
  return daily_report


def create_payment_report(df: pd.DataFrame) -> pd.DataFrame:
  """
  Creates payment report - data grouped by payment type and aggregates
  sum of fare amount and extras and median of fare amount and extras.

  :param df: Input DataFrame.
  :return: Payment report.
  """
  start_create = perf_counter()
  payment_report = df.groupby("payment_type").agg({
    "fare_amount": ["sum", "median"],
    "extra": ["sum", "median"]
  }).reset_index()
  payment_report.columns = ["payment_type", "fare_amount_sum",
                            "fare_amount_median", "extra_sum", "extra_median"]
  payment_type_map = {
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
  }

  payment_report["payment_type"] = payment_report["payment_type"].replace(
    payment_type_map)
  end_create = perf_counter()
  logger.info(f"Creating payment report time: {end_create - start_create}")
  return payment_report


@measure_resources(interval=0.1)
def transform_main(
  scenario: str = "first", **kwargs
) -> None:  # pragma: no cover
  """
  Main transformation function.

  :param scenario: Scenario that will be performed.
  :param kwargs: Config dictionary.
  """
  start_transform = perf_counter()
  if scenario == "first":
    logger.info("Performing benchmark in first scenario")
    df = pd.read_parquet(f"{kwargs['data_path']}yellow_tripdata_2025-06.parquet")
  elif scenario == "second":
    logger.info("Performing benchmark in second scenario")
    df = pd.DataFrame()
    files_to_transform = [
      "yellow_tripdata_2025-06.parquet",
      "yellow_tripdata_2025-07.parquet",
      "yellow_tripdata_2025-08.parquet",
    ]
    for file in files_to_transform:
      df = pd.concat([df, pd.read_parquet(f"{kwargs['data_path']}{file}")])
  else:
    raise TypeError(f"Scenario: {scenario} is not supported!")

  location_map = pd.read_csv(
    f"{kwargs['data_path']}taxi_zone_lookup.csv",
    usecols=["LocationID", "Zone"]
  )
  df = preprocess(df)
  df = calculate_speed(df)
  df = calculate_tip_ratio(df)
  daily_report = create_daily_report(df, location_map)
  payment_report = create_payment_report(df)
  daily_report.to_parquet(f"{kwargs['data_path']}daily_report.parquet")
  payment_report.to_parquet(f"{kwargs['data_path']}payment_report.parquet")
  end_transform = perf_counter()
  logger.info(f"Main transformation time: {end_transform - start_transform}")
