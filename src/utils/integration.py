"""Reusable contextual joins, independent of dataset names and schemas."""

from functools import reduce
from operator import and_

from pyspark.sql import functions as F


def join_context(left, right, keys, columns, prefix, broadcast=False):
    """Left join context using {left_key: right_key} and prefix selected columns.

    Keys must be nonempty and output names must not overlap existing columns.
    The caller is responsible for checking that joins preserve the row count.
    """
    left = left.alias("base")
    right = (F.broadcast(right) if broadcast else right).alias("context")
    condition = reduce(and_, [
        F.col(f"base.`{a}`") == F.col(f"context.`{b}`")
        for a, b in keys.items()
    ])
    return left.join(right, condition, "left").select(
        "base.*",
        *[F.col(f"context.`{column}`").alias(f"{prefix}_{column}") for column in columns],
    )
