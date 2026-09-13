"""Ingest all datasets and build the integrated taxi-trip Delta table."""

from src.pipelines import ingest, integrate
from src.spark import create_delta_spark


def main():
    spark = create_delta_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ingest.run(spark)
        integrate.run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
