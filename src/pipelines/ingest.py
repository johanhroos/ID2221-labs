"""Ingest all datasets, or one source/config pair."""

import argparse
import subprocess

from src.utils.ingestion import ingest_dataset
from src.config import DATA_DIR, CONFIG_DIR, DATASETS
from src.spark import create_delta_spark


def run(spark, inputs=None):
    """Ingest source/config pairs, defaulting to all configured datasets."""
    if inputs is None:
        inputs = []
        for dataset in DATASETS.values():
            source = DATA_DIR / dataset["source"]
            if "archive" in dataset and not source.is_file():
                subprocess.run(
                    ["unzip", str(DATA_DIR / dataset["archive"]), "-d", str(DATA_DIR)],
                    check=True,
                )
            inputs.append((str(source), str(CONFIG_DIR / dataset["config"])))

    spark.sql("CREATE DATABASE IF NOT EXISTS id2221_lab")
    spark.sql("USE id2221_lab")
    for dataset_path, config_path in inputs:
        ingest_dataset(spark, dataset_path, config_path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset_path", nargs="?")
    parser.add_argument("config_path", nargs="?")
    args = parser.parse_args()
    if bool(args.dataset_path) != bool(args.config_path):
        parser.error("Supply both dataset_path and config_path, or neither")
    inputs = [(args.dataset_path, args.config_path)] if args.dataset_path else None

    spark = create_delta_spark()
    try:
        run(spark, inputs)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
