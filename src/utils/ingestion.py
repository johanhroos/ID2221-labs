"""Generic configured ingestion for CSV and Parquet datasets."""

import json
import re
import time
import uuid
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from src.config import CLEANED_DIR, INGESTION_METADATA_TABLE
from src.datasets import execute_dataspecific_functions
from src.spark import write_delta
from src.utils.ingestion_logger import IngestionLogger
from src.utils.keys import add_generated_key, validate_primary_key


SPARK_TYPES = {
    "string": StringType(),
    "int": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampNTZType(),
}


def _format_column_name(name):
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_").lower()


def load_config(config_path):
    with Path(config_path).open(encoding="utf-8") as handle:
        return json.load(handle)


def _resolve_source_files(dataset_path):
    path = Path(dataset_path)
    if any(character in path.name for character in "*?[]"):
        files = sorted(
            str(candidate)
            for candidate in path.parent.iterdir()
            if candidate.is_file() and fnmatch(candidate.name, path.name)
        )
    else:
        files = [str(path)] if path.is_file() else []
    if not files:
        raise FileNotFoundError(f"No data files match {dataset_path}")
    return files


def _load_source(spark, dataset_path, source_format):
    files = _resolve_source_files(dataset_path)
    if source_format == "csv":
        return spark.read.option("header", True).option("mode", "FAILFAST").csv(files)
    if source_format == "parquet":
        return spark.read.parquet(*files)
    raise ValueError(f"Unsupported input format: {source_format}")


def _get_initial_schema(column_config):
    return StructType([
        StructField(
            column["name"],
            SPARK_TYPES[column["type"].lower()],
            nullable=column["nullable"],
        )
        for column in column_config
    ])


def _validate_and_cast(df, column_config):
    # Read source names first so schema errors cannot silently mislabel values.
    expected = [column["name"] for column in column_config]
    missing = [name for name in expected if name not in df.columns]
    unexpected = [name for name in df.columns if name not in expected]
    if missing or unexpected:
        raise ValueError(
            f"Input schema differs from configuration: missing={missing}, "
            f"unexpected={unexpected}"
        )

    converted = []
    invalid_cast = F.lit(False)
    for field in _get_initial_schema(column_config):
        source = F.col(f"`{field.name}`")
        value = F.when(F.trim(source.cast("string")) == "", None).otherwise(source)
        casted = value.cast(field.dataType)
        invalid_cast = invalid_cast | (value.isNotNull() & casted.isNull())
        converted.append(casted.alias(field.name))

    return (
        df.select(*converted, invalid_cast.alias("_invalid_schema"))
        .filter(~F.col("_invalid_schema"))
        .drop("_invalid_schema")
    )


def _format_column_names(df, config):
    names = [
        _format_column_name(column.get("rename_to", column["name"]))
        for column in config["columns"]
    ]
    if any(not name or name[0].isdigit() for name in names) or len(names) != len(set(names)):
        raise ValueError("Column names must be valid and distinct after formatting")
    # The validation step selects columns in configuration order.
    df = df.toDF(*names)
    for column_config, name in zip(config["columns"], names):
        column_config["name"] = name
    return df, config


def _handle_null_entries(df, config, logger=None):
    should_log = logger is not None and logger.enabled
    for column_config in config["columns"]:
        if not column_config["nullable"]:
            before_count = df.count() if should_log else None
            name = column_config["name"]
            if "default" in column_config:
                df = df.withColumn(
                    name,
                    F.when(F.col(name).isNull(), F.lit(column_config["default"]))
                    .otherwise(F.col(name))
                    .cast(SPARK_TYPES[column_config["type"].lower()]),
                )
            else:
                df = df.filter(F.col(name).isNotNull())
            if should_log:
                after_count = df.count()
                logger.removed(
                    f"null values in non-nullable column {name}",
                    before_count - after_count, before_count, after_count,
                )
    return df


def _remove_unwanted_columns(df, config):
    for column_config in config["columns"]:
        if not column_config["keep_column"]:
            df = df.drop(column_config["name"])
    return df


def apply_quality_rules(df, rules):
    """Remove rows violating simple, reusable rules declared in dataset JSON."""
    valid = F.lit(True)
    for rule in rules:
        column = F.col(rule["column"])
        match rule["rule"]:
            case "not_null":
                invalid = column.isNull()
                if isinstance(df.schema[rule["column"]].dataType, StringType):
                    invalid = invalid | (F.trim(column) == "")
            case "finite":
                invalid = F.isnan(column) | (F.abs(column) == float("inf"))
            case "minimum":
                invalid = column < F.lit(rule["value"])
            case "maximum":
                invalid = column > F.lit(rule["value"])
            case "between":
                invalid = (
                    (column < F.lit(rule["minimum"]))
                    | (column > F.lit(rule["maximum"]))
                )
            case "after_or_equal":
                invalid = column < F.col(rule["other_column"])
            case _:
                raise ValueError(f"Unknown quality rule: {rule['rule']}")
        valid = valid & ~F.coalesce(invalid, F.lit(False))
    return df.filter(valid)


def prepare_dataset(spark, dataset_path, config_path, logger=None):
    """Load and transform one source without writing it."""
    should_log = logger is not None and logger.enabled
    if should_log:
        logger.step("configuration", f"Reading {config_path}")
    config = load_config(config_path)
    if should_log:
        logger.set_table_name(config["table_name"])
        logger.step("load", "Reading source data")
    raw = _load_source(spark, dataset_path, config["format"])
    input_records = raw.count()
    if should_log:
        logger.info("Loaded %s row(s)", f"{input_records:,}")

    before = input_records
    df = _validate_and_cast(raw, config["columns"])
    if should_log:
        after = df.count()
        logger.removed("invalid schema values", before - after, before, after)
    if should_log:
        logger.step("column formatting", "Normalizing source column names")
    df, config = _format_column_names(df, config)

    if should_log:
        logger.step("null handling", "Applying nullable-column rules")
    df = _handle_null_entries(df, config, logger)

    df = execute_dataspecific_functions(df, config, logger)
    if should_log:
        before = df.count()

    df = _remove_unwanted_columns(df, config)
    if should_log:
        logger.step("column cleanup", "Removed columns marked keep_column=false")

    df = apply_quality_rules(df, config.get("quality_rules", []))
    if should_log:
        after = df.count()
        logger.removed("configured quality rules", before - after, before, after)
        before = after

    df = df.dropDuplicates()
    if should_log:
        after = df.count()
        logger.removed("exact duplicate rows", before - after, before, after)
        before = after

    df = add_generated_key(df, config)
    if should_log:
        logger.step("key generation", "Added the configured generated key")

    if should_log:
        logger.step("primary-key validation", "Validating the cleaned DataFrame")
    validate_primary_key(df, config)
    return df, config, input_records


def _write_metadata(spark, metadata):
    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("source_path", StringType(), False),
        StructField("started_at_utc", StringType(), False),
        StructField("execution_seconds", DoubleType(), False),
        StructField("input_records", LongType(), False),
        StructField("processed_records", LongType(), False),
        StructField("rejected_records", LongType(), False),
        StructField("schema_version", IntegerType(), False),
    ])
    write_delta(
        spark.createDataFrame([metadata], schema),
        INGESTION_METADATA_TABLE,
        mode="append",
    )


def ingest_dataset(spark, dataset_path, config_path, enable_logging=True):
    """Process one dataset, write its cleaned Delta table, and record metadata."""
    started_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    with IngestionLogger(dataset_path, enabled=enable_logging) as logger:
        df, config, input_records = prepare_dataset(
            spark, dataset_path, config_path, logger
        )
        df = df.persist(StorageLevel.DISK_ONLY)
        try:
            processed_records = df.count()
            write_delta(df, CLEANED_DIR / config["table_name"])
            metadata = {
                "run_id": str(uuid.uuid4()),
                "table_name": config["table_name"],
                "source_path": str(dataset_path),
                "started_at_utc": started_at.isoformat(),
                "execution_seconds": time.perf_counter() - started,
                "input_records": input_records,
                "processed_records": processed_records,
                "rejected_records": input_records - processed_records,
                "schema_version": config["schema_version"],
            }
            _write_metadata(spark, metadata)
            logger.info(
                "Processed %s of %s records; rejected %s; schema version %s; %.3f seconds",
                f"{processed_records:,}",
                f"{input_records:,}",
                f"{metadata['rejected_records']:,}",
                metadata["schema_version"],
                metadata["execution_seconds"],
            )
            print(
                f"Wrote {config['table_name']}: {processed_records:,} processed, "
                f"{metadata['rejected_records']:,} rejected"
            )
            return metadata
        finally:
            df.unpersist()
