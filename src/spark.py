"""Spark session setup and shared Delta writing."""

from src import config


def create_delta_spark(
    app_name="DeltaLakeApp", master=None, memory=None, shuffle_partitions=None
):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(config.WAREHOUSE_DIR))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", config.LOCAL_TIMEZONE)
        .config("spark.driver.memory", memory or config.SPARK_MEMORY)
        .config(
            "spark.sql.shuffle.partitions",
            str(shuffle_partitions or config.STANDARD_SPARK_PARTITIONS_DURING_SHUFFLE),
        )
    )
    if master:
        builder = builder.master(master)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_delta(df, path, mode="overwrite", partition_by=(), max_records_per_file=100_000):
    writer = (
        df.write.format("delta").mode(mode)
        .option("maxRecordsPerFile", max_records_per_file)
    )
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))
