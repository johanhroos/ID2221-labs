from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "configs" / "datasets"
CLEANED_DIR = PROJECT_ROOT / "delta_tables" / "cleaned"
INTEGRATED_DIR = PROJECT_ROOT / "delta_tables" / "integrated"
WAREHOUSE_DIR = PROJECT_ROOT / "spark_warehouse"
LOG_DIR = PROJECT_ROOT / "logs"
METADATA_DIR = PROJECT_ROOT / "delta_tables" / "metadata"
INGESTION_METADATA_TABLE = METADATA_DIR / "ingestion_runs"

SPARK_MEMORY = "8g"     # e.g. "8g" is 8GB
STANDARD_SPARK_PARTITIONS_DURING_SHUFFLE = "200"
LOCAL_TIMEZONE = "America/New_York"

# Sources to ingest. Schemas and transformation names remain in their JSON files.
DATASETS = {
    "taxi": {"source": "yellow_tripdata_2024-*.parquet", "config": "taxi_trips.json"},
    "air_quality": {
        "source": "hourly_88101_2024.csv",
        "config": "air_quality.json",
        "archive": "air_quality.zip",
    },
    "weather": {"source": "weather.csv", "config": "weather.json"},
    "zones": {"source": "taxi_zone_lookup.csv", "config": "taxi_zone_lookup.json"},
}

# Include preparation dependencies too: air quality needs the zone counties.
INTEGRATION_INPUTS = ["taxi", "zones", "weather", "air_quality"]
INTEGRATION_OUTPUT = INTEGRATED_DIR / "integrated_taxi_trips"

ZONE_COLUMNS = ["zone", "borough", "service_zone", "state_code", "county_code"]
WEATHER_COLUMNS = [
    "temp", "rhum", "prcp", "snwd", "wdir", "wspd", "wpgt", "pres", "cldc", "coco",
]
INTEGRATION_JOINS = [
    {
        "dataset": "zones",
        "keys": {"pulocation_id": "location_id"},
        "columns": ZONE_COLUMNS,
        "output_columns": ["zone", "borough"],
        "prefix": "pickup",
        "match_column": "zone",
    },
    {
        "dataset": "zones",
        "keys": {"dolocation_id": "location_id"},
        "columns": ZONE_COLUMNS,
        "output_columns": ["zone", "borough"],
        "prefix": "dropoff",
        "match_column": "zone",
    },
    {
        "dataset": "weather",
        "keys": {"pickup_hour": "timestamp"},
        "columns": WEATHER_COLUMNS,
        "prefix": "weather",
        "match_column": "temp",
    },
    {
        "dataset": "air_quality",
        "keys": {
            "pickup_state_code": "state_code",
            "pickup_county_code": "county_code",
            "pickup_hour": "hour",
        },
        "columns": ["measurement", "parameter_name", "units_of_measure"],
        "prefix": "air_quality",
        "match_column": "measurement",
    },
]
