"""Generate configured record IDs and validate keys on cleaned data."""

from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, StringType


def add_generated_key(df, config):
    """Add the optional configured generated key."""
    rule = config.get("generated_key")
    if rule is None:
        return df

    name = rule["name"]
    columns = rule["columns"]
    if not columns or len(columns) != len(set(columns)):
        raise ValueError("generated_key.columns must be nonempty and distinct")
    if name in df.columns:
        raise ValueError(f"Generated key would overwrite existing column: {name}")
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"Generated key columns are missing: {sorted(missing)}")

    # Explicit ordering and null-preserving JSON avoid delimiter/null ambiguity.
    # The configured fields and their standardized types define record identity.
    payload = F.to_json(
        F.struct(*[F.col(column) for column in columns]),
        options={"ignoreNullFields": "false", "timeZone": "UTC"},
    )
    return df.withColumn(name, F.sha2(payload, 256))


def validate_primary_key(df, config):
    columns = config.get("primary_key")
    if (not isinstance(columns, list) or not columns
            or not all(isinstance(column, str) for column in columns)
            or len(columns) != len(set(columns))):
        raise ValueError("primary_key must be a nonempty list of distinct column names")
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"Primary key columns are missing: {sorted(missing)}")

    conditions = []
    for name in columns:
        column = F.col(name)
        condition = column.isNull()
        dtype = df.schema[name].dataType
        if isinstance(dtype, StringType):
            condition = condition | (F.trim(column) == "")
        elif isinstance(dtype, (FloatType, DoubleType)):
            condition = condition | F.isnan(column)
        conditions.append(condition)
    missing_key = reduce(lambda left, right: left | right, conditions)

    # Aggregate just the keys, not the wide dataset. Counts include all rows in
    # each missing-key group, and extra rows beyond one for duplicate keys.
    groups = df.groupBy(*columns).agg(F.count(F.lit(1)).alias("_key_count"))
    stats = groups.agg(
        F.coalesce(F.sum(F.when(missing_key, F.col("_key_count")).otherwise(0)), F.lit(0))
        .alias("missing_key_rows"),
        F.count(F.when(F.col("_key_count") > 1, 1)).alias("duplicate_key_groups"),
        F.coalesce(F.sum(F.col("_key_count") - 1), F.lit(0)).alias("duplicate_key_rows"),
    ).first().asDict()

    if stats["missing_key_rows"] or stats["duplicate_key_groups"]:
        examples = [row.asDict() for row in groups.filter(
            missing_key | (F.col("_key_count") > 1)
        ).limit(5).collect()]
        raise ValueError(
            f"Primary key validation failed for {config['table_name']} {columns}: "
            f"{stats}; examples={examples}"
        )
    print(f"Primary key validated for {config['table_name']}: {columns}")
    return stats
