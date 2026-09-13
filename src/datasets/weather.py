"""Weather timestamp preparation."""

from pyspark.sql import functions as F

def separate_time_to_timestamp(df):
    has_unit = {
        "year": "year" in df.columns,
        "month": "month" in df.columns,
        "day": "day" in df.columns,
        "hour": "hour" in df.columns,
        "minute": "minute" in df.columns,
        "second": "second" in df.columns
    }
    df = df.withColumn(
        "timestamp",
        F.make_timestamp_ntz(
            F.col("year") if has_unit["year"] else F.lit(0),
            F.col("month") if has_unit["month"] else F.lit(0),
            F.col("day") if has_unit["day"] else F.lit(0),
            F.col("hour") if has_unit["hour"] else F.lit(0),
            F.col("minute") if has_unit["minute"] else F.lit(0),
            F.col("second") if has_unit["second"] else F.lit(0)
        )
    )
    # Non-null components can still describe an invalid date (e.g. February 30).
    df = df.filter(F.col("timestamp").isNotNull())

    # remove the time units as they are unnessary now
    for time_unit in has_unit:
        if has_unit[time_unit]:
            df = df.drop(time_unit)
    return df
