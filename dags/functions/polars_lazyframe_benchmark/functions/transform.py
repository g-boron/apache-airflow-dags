""" Transforming data """
from time import perf_counter
import logging
from datetime import timedelta

import polars as pl

from functions.operational_functions.metrics.resources import (
  measure_resources
)

logger = logging.getLogger("airflow.task")


def preprocess(lf: pl.LazyFrame) -> pl.LazyFrame:
  """
  Preprocess data - drops empty passenger count and creates date related
  columns.

  :param lf: Input LazyFrame.
  :return: Preprocessed LazyFrame.
  """
  start_preprocess = perf_counter()
  lf = lf.drop_nulls(subset=["passenger_count"])

  lf = lf.with_columns([
    pl.col("tpep_pickup_datetime").dt.date().alias("pickup_date"),
    pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
    pl.col("tpep_pickup_datetime").dt.strftime("%A").alias("pickup_day"),
    pl.col("tpep_dropoff_datetime").dt.date().alias("dropoff_date"),
    pl.col("tpep_dropoff_datetime").dt.hour().alias("dropoff_hour"),
    pl.col("tpep_dropoff_datetime").dt.strftime("%A").alias("dropoff_day"),
  ])

  end_preprocess = perf_counter()
  logger.info(f"Preprocess time: {end_preprocess - start_preprocess}")
  return lf


def calculate_speed(lf: pl.LazyFrame) -> pl.LazyFrame:
  """
  Calculates taxi speed in mph. Removes anomalies - average speed more than
  100 mph, trip distance equals to 0 miles or trip duration less than 1 minute.

  :param lf: Input LazyFrame.
  :return: LazyFrame with calculated taxi speed.
  """
  start_calculate = perf_counter()

  lf = lf.with_columns([
    (pl.col("tpep_dropoff_datetime") - pl.col(
      "tpep_pickup_datetime")).alias(
      "duration"),
  ])

  lf = lf.with_columns([
    pl.when(pl.col("duration") <= timedelta(seconds=0))
    .then(None)
    .otherwise(pl.col("duration")).alias("duration"),
    (pl.col("duration").dt.total_seconds() / 3600).alias("duration_hours")
  ])

  lf = lf.with_columns([
    (pl.col("trip_distance") / pl.col("duration_hours")).alias("avg_speed")
  ])

  lf = lf.with_columns([
    pl.when((pl.col("avg_speed") > 100) | (
        pl.col("duration") < timedelta(seconds=60)) | (
                pl.col("trip_distance") == 0)).then(None).otherwise(
      pl.col("avg_speed")).alias("avg_speed")
  ])

  end_calculate = perf_counter()
  logger.info(f"Calculating speed time: {end_calculate - start_calculate}")
  return lf


def calculate_tip_ratio(lf: pl.LazyFrame) -> pl.LazyFrame:
  """
  Calculates tip ratio by dividing tip amount by fare amount. Removes
  anomalies - tip ratio less than 0, fare amount less than 1 USD and tip ratio
  more than 100.

  :param lf: Input LazyFrame.
  :return: LazyFrame with calculated tip ratio.
  """
  start_calculate = perf_counter()

  lf = lf.with_columns([
    (pl.col("tip_amount") / pl.col("fare_amount")).alias("tip_ratio")
  ])
  lf = lf.with_columns([
    pl.when(
      (pl.col("tip_ratio") < 0) | (pl.col("fare_amount") < 1) | (
          pl.col("tip_ratio") > 100)
    ).then(None).otherwise(pl.col("tip_ratio")).alias("tip_ratio")
  ])

  end_calculate = perf_counter()
  logger.info(f"Calculating tip ratio time: {end_calculate - start_calculate}")
  return lf


def create_daily_report(
  lf: pl.LazyFrame, location_map: pl.LazyFrame
) -> pl.DataFrame:
  """
  Creates daily report - data grouped by pickup date, pickup day, departure
  location and arrival location and aggregates mean trip distance, fare amount,
  tip amount, tip ratio and average speed, sum of tip amount and passenger
  count and count of trips.

  :param lf: Input LazyFrame.
  :param location_map: Locations LazyFrame map.
  :return: Daily report.
  """
  start_create = perf_counter()
  lf = lf.with_columns([
    pl.col("PULocationID").cast(pl.Int64),
    pl.col("DOLocationID").cast(pl.Int64),
  ])
  daily_report = lf.group_by(
    ["pickup_date", "pickup_day", "PULocationID", "DOLocationID"]).agg(
    pl.col("trip_distance").mean().alias("mean_trip_distance"),
    pl.col("fare_amount").mean().alias("mean_fare_amount"),
    pl.col("tip_amount").mean().alias("mean_tip_amount"),
    pl.col("tip_amount").sum().alias("sum_tip_amount"),
    pl.col("tip_ratio").mean().alias("mean_tip_ratio"),
    pl.col("passenger_count").sum().alias("passenger_count"),
    pl.col("VendorID").count().alias("trips_count"),
    pl.col("avg_speed").mean().alias("mean_speed")
  )
  daily_report = daily_report.rename(
    {"PULocationID": "pickup_location", "DOLocationID": "dropoff_location"})
  daily_report = daily_report.join(location_map, left_on="pickup_location",
                                   right_on="LocationID", how="left")
  daily_report = daily_report.drop(["pickup_location"])
  daily_report = daily_report.rename({"Zone": "pickup_location"})
  daily_report = daily_report.join(location_map, left_on="dropoff_location",
                                   right_on="LocationID", how="left")
  daily_report = daily_report.drop(["dropoff_location"])
  daily_report = daily_report.rename({"Zone": "dropoff_location"})
  daily_report = daily_report.with_columns([
    pl.col("pickup_location").fill_null("Unknown"),
    pl.col("dropoff_location").fill_null("Unknown")
  ])
  daily_report = daily_report.with_columns([
    pl.col("pickup_date").cast(pl.Date)
  ])
  daily_report = daily_report.select([
    "pickup_date", "pickup_day", "pickup_location", "dropoff_location",
    "mean_trip_distance", "mean_fare_amount", "mean_tip_amount",
    "sum_tip_amount", "mean_tip_ratio", "passenger_count", "trips_count",
    "mean_speed"
  ]).collect()

  end_create = perf_counter()
  logger.info(f"Creating daily report time: {end_create - start_create}")
  return daily_report


def create_payment_report(lf: pl.LazyFrame) -> pl.DataFrame:
  """
  Creates payment report - data grouped by payment type and aggregates
  sum of fare amount and extras and median of fare amount and extras.

  :param lf: Input LazyFrame.
  :return: Payment report.
  """
  start_create = perf_counter()

  payment_report = lf.group_by("payment_type").agg(
    pl.col("fare_amount").sum().alias("fare_amount_sum"),
    pl.col("fare_amount").median().alias("fare_amount_median"),
    pl.col("extra").sum().alias("extra_sum"),
    pl.col("extra").median().alias("extra_median"),
  )
  payment_type_map = {
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
  }
  payment_report = payment_report.with_columns([
    pl.col("payment_type").cast(pl.Utf8).replace(payment_type_map).alias(
      "payment_type")
  ]).collect()

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
    lf = pl.scan_parquet(
      f"{kwargs['data_path']}yellow_tripdata_2025-06.parquet")
  elif scenario == "second":
    logger.info("Performing benchmark in second scenario")
    files_to_transform = [
      "yellow_tripdata_2025-06.parquet",
      "yellow_tripdata_2025-07.parquet",
      "yellow_tripdata_2025-08.parquet",
    ]
    lf_list = [
      pl.scan_parquet(f"{kwargs['data_path']}{file}")
      for file in files_to_transform
    ]
    lf = pl.concat(lf_list)
  else:
    raise TypeError(f"Scenario: {scenario} is not supported!")

  location_map = pl.scan_csv(f"{kwargs['data_path']}taxi_zone_lookup.csv").select([
    "LocationID", "Zone"
  ])

  df = preprocess(lf)
  df = calculate_speed(df)
  df = calculate_tip_ratio(df)

  daily_report = create_daily_report(df, location_map)
  payment_report = create_payment_report(df)

  daily_report.write_parquet(f"{kwargs['data_path']}daily_report.parquet")
  payment_report.write_parquet(f"{kwargs['data_path']}payment_report.parquet")

  end_transform = perf_counter()
  logger.info(f"Main transformation time: {end_transform - start_transform}")
