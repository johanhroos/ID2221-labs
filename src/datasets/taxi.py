"""Taxi quality flags and pickup-hour preparation."""

from pyspark.sql import functions as F

def add_negative_fare_flag(df):
    """Retain whether the reported meter fare is negative for quality analysis."""
    if "fare_amount" not in df.columns:
        raise ValueError("Cannot derive negative_fare: fare_amount is missing")
    return df.withColumn("negative_fare", F.col("fare_amount") < F.lit(0))


def add_likely_reimbursement_flag(df):
    """Flag records whose reported total amount is negative."""
    if "total_amount" not in df.columns:
        raise ValueError("Cannot derive likely_reimbursement: total_amount is missing")
    return df.withColumn("likely_reimbursement", F.col("total_amount") < F.lit(0))


def prepare_pickup_hour(taxi):
    # Assume source timestamps share a local wall clock; DST is not resolved here.
    return taxi.withColumn(
        "pickup_hour",
        F.date_trunc("hour", "tpep_pickup_datetime").cast("timestamp_ntz"),
    )
