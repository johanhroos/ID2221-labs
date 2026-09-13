"""Task 5: prepare contextual data, join taxi trips, and write Delta."""

import json

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.datasets.air_quality import prepare_air_quality
from src.datasets.taxi import prepare_pickup_hour
from src.utils.integration import join_context
from src.config import (
    CLEANED_DIR, CONFIG_DIR, DATASETS, INTEGRATION_INPUTS, INTEGRATION_JOINS,
    INTEGRATION_OUTPUT, LOCAL_TIMEZONE,
)
from src.spark import create_delta_spark, write_delta


def build_integrated_taxi_trips(tables):
    """Prepare taxi context and apply the configured left joins in order."""
    context = dict(tables)
    if any(join["dataset"] == "air_quality" for join in INTEGRATION_JOINS):
        context["air_quality"] = prepare_air_quality(
            tables["air_quality"], tables["zones"],
            local_timezone=LOCAL_TIMEZONE,
        )
    integrated = prepare_pickup_hour(tables["taxi"])
    for join in INTEGRATION_JOINS:
        right = context[join["dataset"]]
        integrated = join_context(
            integrated, right, join["keys"], join["columns"], join["prefix"],
            broadcast=join.get("broadcast", True),
        )
    context_columns = [
        f"{join['prefix']}_{column}"
        for join in INTEGRATION_JOINS
        for column in join.get("output_columns", join["columns"])
    ]
    return integrated.select(*tables["taxi"].columns, *context_columns)


def run(spark):
    """Build and write the integrated table using the caller's Spark session."""
    tables = {}
    for name in INTEGRATION_INPUTS:
        with (CONFIG_DIR / DATASETS[name]["config"]).open(encoding="utf-8") as handle:
            table_name = json.load(handle)["table_name"]
        tables[name] = spark.read.format("delta").load(str(CLEANED_DIR / table_name))

    input_rows = tables["taxi"].count()
    integrated = build_integrated_taxi_trips(tables).persist(StorageLevel.DISK_ONLY)
    match_columns = {
        join["prefix"]: f"{join['prefix']}_{join['match_column']}"
        for join in INTEGRATION_JOINS
    }
    metrics = integrated.agg(
        F.count("*").alias("rows"),
        *[F.count(column).alias(label) for label, column in match_columns.items()],
    ).first()
    if metrics["rows"] != input_rows:
        raise ValueError(
            f"Joins changed the taxi row count: {input_rows:,} -> {metrics['rows']:,}. "
            "Check for duplicate context join keys."
        )

    write_delta(integrated, INTEGRATION_OUTPUT)
    integrated.unpersist()
    print(f"Created {INTEGRATION_OUTPUT} with {input_rows:,} trips")
    for label in match_columns:
        matched = metrics[label]
        percentage = 100 * matched / input_rows if input_rows else 0
        print(f"{label} matches: {matched:,} ({percentage:.2f}%)")


def main():
    spark = create_delta_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
