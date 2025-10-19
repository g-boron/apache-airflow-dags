from time import perf_counter
import logging
from datetime import timedelta

import polars as pl

logger = logging.getLogger("airflow.task")


def preprocess(df: pl.DataFrame) -> pl.DataFrame:
    start_preprocess = perf_counter()
    logger.info(f"DF shape before dropna: {df.shape}")
    df = df.drop_nulls(subset=["passenger_count"])
    logger.info(f"DF shape after dropna: {df.shape}")

    df = df.with_columns([
        df["tpep_pickup_datetime"].dt.date().alias("pickup_date"),
        df["tpep_pickup_datetime"].dt.hour().alias("pickup_hour"),
        df["tpep_pickup_datetime"].dt.strftime("%A").alias("pickup_day"),
        df["tpep_dropoff_datetime"].dt.date().alias("dropoff_date"),
        df["tpep_dropoff_datetime"].dt.hour().alias("dropoff_hour"),
        df["tpep_dropoff_datetime"].dt.strftime("%A").alias("dropoff_day"),
    ])

    end_preprocess = perf_counter()
    logger.info(f"Preprocess time: {end_preprocess - start_preprocess}")
    return df


def calculate_speed(df: pl.DataFrame) -> pl.DataFrame:
    start_calculate = perf_counter()

    df = df.with_columns([
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).alias(
            "duration"),
    ])
    df = df.with_columns([
        pl.when(df["duration"] <= timedelta(seconds=0)).then(None).otherwise(
            df["duration"]).alias("duration"),
        (df["duration"].dt.total_seconds() / 3600).alias("duration_hours")
    ])
    df = df.with_columns([
        (df["trip_distance"] / df["duration_hours"]).alias("avg_speed")
    ])
    df = df.with_columns([
        pl.when((df["avg_speed"] > 100) | (
                df["duration"] < timedelta(seconds=60)) | (
                        df["trip_distance"] == 0)).then(None).otherwise(
            df["avg_speed"]).alias("avg_speed")
    ])

    end_calculate = perf_counter()
    logger.info(f"Calculating speed time: {end_calculate - start_calculate}")
    return df


def calculate_tip_ratio(df: pl.DataFrame) -> pl.DataFrame:
    start_calculate = perf_counter()

    df = df.with_columns([
        (df["tip_amount"] / df["fare_amount"]).alias("tip_ratio")
    ])
    df = df.with_columns([
        pl.when(
            (df["tip_ratio"] < 0) | (df["fare_amount"] < 1) | (
                    df["tip_ratio"] > 100)
        ).then(None).otherwise(df["tip_ratio"]).alias("tip_ratio")
    ])

    end_calculate = perf_counter()
    logger.info(f"Calculating tip ratio time: {end_calculate - start_calculate}")
    return df


def create_daily_report(df: pl.DataFrame, location_map: pl.DataFrame) -> pl.DataFrame:
    start_create = perf_counter()
    df = df.with_columns([
        pl.col("PULocationID").cast(pl.Int64),
        pl.col("DOLocationID").cast(pl.Int64),
    ])
    daily_report = df.group_by(
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
        daily_report["pickup_location"].fill_null("Unknown"),
        daily_report["dropoff_location"].fill_null("Unknown")
    ])
    daily_report = daily_report.with_columns([
        pl.col("pickup_date").cast(pl.Datetime)
    ])
    daily_report = daily_report.select([
        "pickup_date", "pickup_day", "pickup_location", "dropoff_location",
        "mean_trip_distance", "mean_fare_amount", "mean_tip_amount",
        "sum_tip_amount", "mean_tip_ratio", "passenger_count", "trips_count",
        "mean_speed"
    ])

    end_create = perf_counter()
    logger.info(f"Creating daily report time: {end_create - start_create}")
    return daily_report


def create_payment_report(df: pl.DataFrame) -> pl.DataFrame:
    start_create = perf_counter()

    payment_report = df.group_by("payment_type").agg(
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
    ])

    end_create = perf_counter()
    logger.info(f"Creating payment report time: {end_create - start_create}")
    return payment_report


def transform_main(**kwargs) -> None:
    start_transform = perf_counter()

    df = pl.read_parquet(f"{kwargs['data_path']}yellow_tripdata_2025-06.parquet")
    location_map = pl.read_csv(
        f"{kwargs['data_path']}taxi_zone_lookup.csv",
        columns=["LocationID", "Zone"]
    )

    df = preprocess(df)
    df = calculate_speed(df)
    df = calculate_tip_ratio(df)

    daily_report = create_daily_report(df, location_map)
    payment_report = create_payment_report(df)

    daily_report.write_parquet(f"{kwargs['data_path']}daily_report.parquet")
    payment_report.write_parquet(f"{kwargs['data_path']}payment_report.parquet")

    end_transform = perf_counter()
    logger.info(f"Main transformation time: {end_transform - start_transform}")
    # TODO: Osobny DAG na Polars LazyFrame


if __name__ == "__main__":
    KWARGS = {
        "data_path": "X:\\pythonProject\\apache-airflow-dags\\dags\\functions\\pandas_benchmark\\data\\"
    }
    transform_main(**KWARGS)
