# ID2221 Lab — Urban Data Platform

A Spark and Delta Lake project that combines taxi trips with weather, air quality, and zone information.

## What the platform does

- Reads dataset schemas and rules from JSON configuration files.
- Loads taxi Parquet files and weather, air-quality, and zone CSV files.
- Checks source columns, standardizes names and types, and handles missing values.
- Applies dataset transformations, quality rules, exact-row deduplication, and key validation.
- Saves four cleaned Delta tables and records ingestion counts, duration, and schema version.
- Enriches each cleaned taxi trip with pickup/dropoff geography, hourly weather, and county/hour air quality.
- Preserves trips with missing context and checks that joins do not change the trip count.
- Benchmarks the same ingestion code with unpartitioned and monthly-partitioned taxi storage.

## Setup

Install Docker Engine on Linux or Docker Desktop on Windows/macOS. Run the following commands from the repository root.

### Linux and macOS

```bash
docker build --build-arg USER_UID="$(id -u)" --build-arg USER_GID="$(id -g)" -t id2221-lab-container .
docker run --rm -it -v "$PWD:/app" id2221-lab-container
```

### Windows PowerShell

```powershell
docker build -t id2221-lab-container .
docker run --rm -it -v "${PWD.Path}:/app" id2221-lab-container
```

The image provides Python, Java, Spark 3.5.6, and Delta Lake 3.2.0. Rebuild it when the Dockerfile changes.

## Input data

Place the supplied course datasets in `data/`:

```text
data/
  yellow_tripdata_2024-01.parquet
  yellow_tripdata_2024-02.parquet
  yellow_tripdata_2024-03.parquet
  weather.csv
  taxi_zone_lookup.csv
  air_quality.zip
```

Ingestion reads all files matching `yellow_tripdata_2024-*.parquet`. It extracts `hourly_88101_2024.csv` from the ZIP if that CSV is missing.

## Run the pipeline

Run Python commands from the repository root inside the container:

```bash
python3 -m src.run
```

This ingests all four datasets and then integrates them using one Spark session. If ingestion fails, integration does not run. Reruns overwrite the target cleaned and integrated tables and append ingestion metadata.

To run stages separately:

```bash
python3 -m src.pipelines.ingest
python3 -m src.pipelines.integrate
```

Integration requires the cleaned tables. To ingest one dataset:

```bash
python3 -m src.pipelines.ingest data/weather.csv configs/datasets/weather.json
```

Exact duplicate rows are removed. Missing or conflicting primary keys that remain after cleaning cause validation to fail.

## Run benchmarks

Run all supplied taxi months, then January alone for comparison:

```bash
python3 -m src.pipelines.benchmark
python3 -m src.pipelines.benchmark --taxi-path 'data/yellow_tripdata_2024-01.parquet'
```

Each run compares unpartitioned and pickup-year/month Delta tables, using three write trials per layout and three timed repetitions of each query:

- Number of trips per pickup borough.
- Average trip duration per pickup day.
- Average fare per pickup borough.

Results include ingestion time, storage size, file count, and query latency. Run benchmarks sequentially. Use `python3 -m src.pipelines.benchmark --help` for memory, input, and repetition settings.

## Outputs

| Path | Contents |
| --- | --- |
| `delta_tables/cleaned/` | Taxi, weather, air-quality, and zone Delta tables |
| `delta_tables/integrated/integrated_taxi_trips/` | Enriched taxi trips |
| `delta_tables/metadata/ingestion_runs/` | Source, schema version, row counts, start time, and duration |
| `logs/` | Ingestion progress and errors |
| `benchmark_results/<run>/` | Experimental tables, `report.md`, `results.json`, and CSV measurements |

Files persist in the mounted repository after the container exits.

## Code and configuration

```text
configs/datasets/    JSON schemas, keys, and transformation rules
src/run.py          Full-pipeline entry point
src/config.py       Paths, dataset registry, join rules, and Spark settings
src/spark.py        Spark session and shared Delta writer
src/pipelines/      Ingestion, integration, and benchmark entry points
src/datasets/       Dataset-specific transformations
src/utils/          Shared ingestion, joins, keys, and logging
```

To add a dataset, create its JSON configuration and register the source in `src/config.py`. New transformations belong in `src/datasets/` and its dispatcher. Adding a dataset to ingestion does not automatically add it to integration.

The pipeline's default driver memory is 8 GiB, configurable in `src/config.py`. Benchmarks default to 2 GiB and accept `--driver-memory`.
