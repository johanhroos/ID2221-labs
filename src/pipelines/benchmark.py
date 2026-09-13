"""Task 6: repeatable, standalone comparisons of two taxi Delta layouts."""

import argparse
import csv
import glob
import json
import math
import os
import platform
import shutil
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from src.config import CONFIG_DIR, DATA_DIR, PROJECT_ROOT
from src.spark import create_delta_spark, write_delta
from src.utils.ingestion import prepare_dataset


LAYOUTS = {"unpartitioned": [], "pickup_month": ["pickup_year", "pickup_month"]}
QUERIES = ("trips_per_borough", "average_duration_per_day", "average_fare_per_borough")


def create_spark(master="local[2]", memory="2g", shuffle_partitions=8):
    # The existing image's JAVA_HOME is amd64-specific. Resolve its installed
    # Java for this process when running the same image on Apple Silicon.
    java_home = os.environ.get("JAVA_HOME")
    if java_home and not (Path(java_home) / "bin/java").is_file():
        java = shutil.which("java")
        if java:
            os.environ["JAVA_HOME"] = str(Path(java).resolve().parents[1])

    spark = create_delta_spark(
        app_name="ID2221-Task6-Benchmark",
        master=master,
        memory=memory,
        shuffle_partitions=shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def add_partition_columns(df):
    """Add the two columns used by the partitioned benchmark layout."""
    from pyspark.sql import functions as F

    return (df.withColumn("pickup_year", F.year("tpep_pickup_datetime"))
            .withColumn("pickup_month", F.month("tpep_pickup_datetime")))


def load_zones(spark, path):
    from pyspark.sql import functions as F

    zones, _, _ = prepare_dataset(
        spark, str(path), str(CONFIG_DIR / "taxi_zone_lookup.json")
    )
    return zones.select(
        F.col("location_id").alias("pulocation_id"),
        F.trim("borough").alias("borough"),
    )


def query_frame(taxi, zones, name):
    from pyspark.sql import functions as F

    if name == "average_duration_per_day":
        seconds = (F.unix_timestamp("tpep_dropoff_datetime")
                   - F.unix_timestamp("tpep_pickup_datetime"))
        return (taxi.withColumn("duration_seconds", seconds)
                .filter(F.col("duration_seconds") >= 0)
                .groupBy(F.to_date("tpep_pickup_datetime").alias("pickup_day"))
                .agg(F.avg("duration_seconds").alias("average_duration_seconds")))
    joined = taxi.join(F.broadcast(zones), "pulocation_id", "left")
    joined = joined.withColumn("borough", F.when(
        F.col("borough").isNull() | F.col("borough").isin("", "N/A", "Unknown"),
        F.lit("Unknown")).otherwise(F.col("borough")))
    if name == "trips_per_borough":
        return joined.groupBy("borough").agg(F.count("*").alias("trip_count"))
    if name == "average_fare_per_borough":
        fare = F.col("fare_amount")
        finite_fare = F.when(~F.isnan(fare) & (F.abs(fare) != float("inf")), fare)
        return joined.groupBy("borough").agg(F.avg(finite_fare).alias("average_fare"))
    raise ValueError(f"Unknown query: {name}")


def collect_result(df):
    rows = [row.asDict() for row in df.collect()]
    return sorted(rows, key=lambda row: str(next(iter(row.values()))))


def assert_results_equal(expected, actual):
    if len(expected) != len(actual):
        raise AssertionError("Query result row counts differ between layouts/runs")
    for left, right in zip(expected, actual):
        if left.keys() != right.keys():
            raise AssertionError("Query result columns differ")
        for key in left:
            a, b = left[key], right[key]
            if isinstance(a, float) and isinstance(b, float):
                equal = math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-7)
            else:
                equal = a == b
            if not equal:
                raise AssertionError(f"Different query results for {key}: {a!r} != {b!r}")


def write_layout(df, path, layout, write_partitions, max_records_per_file):
    write_delta(
        df.repartition(write_partitions),
        path,
        mode="errorifexists",
        partition_by=LAYOUTS[layout],
        max_records_per_file=max_records_per_file,
    )


def storage_metrics(spark, path):
    from delta.tables import DeltaTable

    detail = DeltaTable.forPath(spark, str(path)).detail().first().asDict()
    files = [p for p in path.rglob("*") if p.is_file()]
    return {"data_bytes": detail["sizeInBytes"], "data_files": detail["numFiles"],
            "total_bytes": sum(p.stat().st_size for p in files), "total_files": len(files)}


def write_csv(path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_report(output, metadata, writes, queries):
    """Write run metadata and measured results; discussion belongs in docs/."""
    lines = [
        "# Taxi storage benchmark — measurements", "",
        f"Run: `{metadata['started_utc']}`. Cleaned input rows: {metadata['input_rows']:,}.", "",
        "## Run metadata", "", "```json", json.dumps(metadata, indent=2, default=str),
        "```", "", "## Storage and ingestion", "",
        "| Layout | Ingestion median (s) | Range (s) | Active data median (MiB) | Active files median | Total size median (MiB) | Total files median |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for layout in LAYOUTS:
        rows = [r for r in writes if r["layout"] == layout]
        med = {k: statistics.median(r[k] for r in rows) for k in
               ("seconds", "data_bytes", "data_files", "total_bytes", "total_files")}
        times = [r["seconds"] for r in rows]
        lines.append(f"| {layout} | {med['seconds']:.3f} | {min(times):.3f}–{max(times):.3f} "
                     f"| {med['data_bytes']/2**20:.2f} | {med['data_files']:g} "
                     f"| {med['total_bytes']/2**20:.2f} | {med['total_files']:g} |")
    lines += ["", "## Query latency", "",
              "| Query | Layout | Median (s) | Min (s) | Max (s) |",
              "| --- | --- | ---: | ---: | ---: |"]
    for name in QUERIES:
        for layout in LAYOUTS:
            times = [r["seconds"] for r in queries if r["query"] == name and r["layout"] == layout]
            lines.append(f"| {name} | {layout} | {statistics.median(times):.3f} "
                         f"| {min(times):.3f} | {max(times):.3f} |")
    (output / "report.md").write_text("\n".join(lines) + "\n")


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--taxi-path", default=str(DATA_DIR / "yellow_tripdata_2024-*.parquet"))
    parser.add_argument("--zones-path", type=Path, default=DATA_DIR / "taxi_zone_lookup.csv")
    parser.add_argument("--output-dir", type=Path,
                        help="New directory; existing directories are never overwritten")
    parser.add_argument("--write-repeats", type=positive_int, default=3)
    parser.add_argument("--query-repeats", type=positive_int, default=3)
    parser.add_argument("--write-partitions", type=positive_int, default=8)
    parser.add_argument("--shuffle-partitions", type=positive_int, default=8)
    parser.add_argument("--max-records-per-file", type=positive_int, default=100000)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="2g")
    return parser.parse_args()


def run(args):
    from importlib.metadata import version
    from pyspark.sql import functions as F

    started = datetime.now(timezone.utc)
    output = (args.output_dir or PROJECT_ROOT / "benchmark_results" /
              started.strftime("%Y%m%dT%H%M%S%fZ")).resolve()
    sources = sorted(glob.glob(args.taxi_path))
    if not sources:
        raise ValueError(f"No taxi input matches {args.taxi_path}")
    source_files = [Path(p) for p in sources]
    if not args.zones_path.is_file():
        raise ValueError(f"Missing taxi-zone CSV: {args.zones_path}")
    output.mkdir(parents=True, exist_ok=False)
    spark = create_spark(args.master, args.driver_memory, args.shuffle_partitions)

    def read_source():
        df, _, _ = prepare_dataset(
            spark, args.taxi_path, str(CONFIG_DIR / "taxi_trips.json")
        )
        return add_partition_columns(df)

    try:
        zones = load_zones(spark, args.zones_path)
        source, _, raw_input_rows = prepare_dataset(
            spark, args.taxi_path, str(CONFIG_DIR / "taxi_trips.json")
        )
        source = add_partition_columns(source)
        duration = (F.unix_timestamp("tpep_dropoff_datetime")
                    - F.unix_timestamp("tpep_pickup_datetime"))
        fare = F.col("fare_amount")
        profile = source.agg(
            F.count("*").alias("input_rows"),
            F.count(F.when(duration.isNull() | (duration < 0), 1)).alias("invalid_duration_rows"),
            F.count(F.when(fare.isNull() | F.isnan(fare) | (F.abs(fare) == float("inf")), 1))
            .alias("invalid_fare_rows"),
            F.min("tpep_pickup_datetime").alias("earliest_pickup"),
            F.max("tpep_pickup_datetime").alias("latest_pickup"),
        ).first().asDict()
        if profile["input_rows"] == 0:
            raise ValueError("Cannot benchmark an empty taxi dataset")
        metadata = {"started_utc": started.isoformat(),
                    "raw_input_rows": raw_input_rows, **profile,
                    "parameters": vars(args), "spark_version": spark.version,
                    "delta_version": version("delta-spark"), "python": platform.python_version(),
                    "platform": platform.platform(), "cpu_count": os.cpu_count(),
                    "spark_master": spark.sparkContext.master,
                    "spark_settings": {k: spark.conf.get(k) for k in (
                        "spark.sql.session.timeZone", "spark.sql.shuffle.partitions",
                        "spark.sql.adaptive.enabled", "spark.driver.memory")},
                    "java_home": os.environ.get("JAVA_HOME"),
                    "input_files": [{"path": str(p.resolve()), "bytes": p.stat().st_size,
                                     "mtime_ns": p.stat().st_mtime_ns} for p in source_files],
                    "zones_path": str(args.zones_path.resolve())}
        print(f"Input: {profile['input_rows']:,} rows. Output: {output}", flush=True)
        writes, queries, paths, expected = [], [], {}, {}
        for trial in range(args.write_repeats):
            order = list(LAYOUTS) if trial % 2 == 0 else list(reversed(LAYOUTS))
            for layout in order:
                path = output / "tables" / f"trial_{trial + 1}" / layout
                spark.catalog.clearCache()
                begin = time.perf_counter()
                write_layout(read_source(), path, layout, args.write_partitions,
                             args.max_records_per_file)
                elapsed = time.perf_counter() - begin
                actual_count = spark.read.format("delta").load(str(path)).count()
                if actual_count != profile["input_rows"]:
                    raise AssertionError(f"{layout}: output row count changed")
                writes.append({"trial": trial + 1, "layout": layout, "seconds": elapsed,
                               "rows": actual_count, **storage_metrics(spark, path)})
                paths[layout] = path
                write_csv(output / "ingestion.csv", writes)
                print(f"Write {trial + 1}/{args.write_repeats} {layout}: {elapsed:.3f}s", flush=True)

        for name in QUERIES:
            for layout in LAYOUTS:
                spark.catalog.clearCache()
                result = collect_result(query_frame(
                    spark.read.format("delta").load(str(paths[layout])), zones, name))
                if name in expected:
                    assert_results_equal(expected[name], result)
                else:
                    expected[name] = result
                if name == "trips_per_borough" and sum(r["trip_count"] for r in result) != profile["input_rows"]:
                    raise AssertionError("Borough join changed the total trip count")
            for trial in range(args.query_repeats):
                order = list(LAYOUTS) if trial % 2 == 0 else list(reversed(LAYOUTS))
                for layout in order:
                    spark.catalog.clearCache()
                    begin = time.perf_counter()
                    result = collect_result(query_frame(
                        spark.read.format("delta").load(str(paths[layout])), zones, name))
                    elapsed = time.perf_counter() - begin
                    assert_results_equal(expected[name], result)
                    queries.append({"query": name, "trial": trial + 1, "layout": layout,
                                    "seconds": elapsed, "result_rows": len(result)})
                    write_csv(output / "queries.csv", queries)
                print(f"Query {name}: repeat {trial + 1}/{args.query_repeats} verified", flush=True)
        (output / "results.json").write_text(json.dumps(
            {"metadata": metadata, "ingestion": writes, "queries": queries,
             "query_results": expected}, indent=2, default=str, allow_nan=False) + "\n")
        write_report(output, metadata, writes, queries)
        print(f"Benchmark complete: {output / 'report.md'}", flush=True)
        return output
    finally:
        spark.stop()


if __name__ == "__main__":
    run(parse_args())
