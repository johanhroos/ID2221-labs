"""Air-quality timestamps and county/hour summaries."""

from pyspark.sql import functions as F

def dates_and_times_to_timestamp(df):
    paired_columns = [
        (column,"date_"+ column[5:]) for column in df.columns
        if column.startswith("time_") and "date_" + column[5:] in df.columns
    ]

    for pair in paired_columns:
        df = df.withColumn(
            "timestamp_" + pair[0][5:],
            F.to_timestamp_ntz(
                F.concat_ws(
                    " ",
                    F.col(pair[1]).cast("string"),
                    F.col(pair[0])
                ),
                F.lit("yyyy-MM-dd HH:mm")
            )
        )
        df = df.drop(pair[0])
        df = df.drop(pair[1])

    return df


def prepare_air_quality(air_quality, zones, local_timezone):
    """Average sensor measurements by pickup county and New York hour."""
    counties = zones.select("state_code", "county_code").distinct()
    relevant = (
        air_quality.join(
            F.broadcast(counties), ["state_code", "county_code"], "left_semi"
        )
        .filter(F.col("timestamp_gmt").isNotNull())
        .withColumn(
            "hour",
            F.date_trunc(
                "hour",
                F.convert_timezone(
                    F.lit("UTC"), F.lit(local_timezone), F.col("timestamp_gmt")
                ),
            ),
        )
    )
    return relevant.groupBy("state_code", "county_code", "hour").agg(
        F.avg("sample_measurement").alias("measurement"),
        F.first("parameter_name").alias("parameter_name"),
        F.first("units_of_measure").alias("units_of_measure"),
    )
