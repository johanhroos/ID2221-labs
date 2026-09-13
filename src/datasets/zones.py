"""Taxi-zone geography rules."""

from pyspark.sql import functions as F

def add_county_from_borough(df):
    if "borough" not in df.columns:
        print("Couldn't add county code as no column 'borough' exists.")
        return df

    borough_to_county = {
        "Queens":"081",
        "Bronx":"005",
        "Manhattan":"061",
        "Staten Island":"085",
        "Brooklyn":"047",
        "EWR":"013"}
    borough_to_state = {
        "Queens":"36",
        "Bronx":"36",
        "Manhattan":"36",
        "Staten Island":"36",
        "Brooklyn":"36",
        "EWR":"34"}

    map_to_county = F.create_map(
        *[F.lit(value) for pair in borough_to_county.items() for value in pair]
    )
    map_to_state = F.create_map(
        *[F.lit(value) for pair in borough_to_state.items() for value in pair]
    )

    # create a new column that has the mapped value from borough to county code
    df = df.withColumn("state_code",
        F.coalesce(
            F.element_at(map_to_state, F.col("borough")),
            F.lit(None)
        )
    )

    df = df.withColumn("county_code",
        F.coalesce(
            F.element_at(map_to_county, F.col("borough")),
            F.lit(None)
        )
    )
    return df
