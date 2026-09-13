"""Dataset-specific transformations referenced by ingestion configurations."""

from .air_quality import dates_and_times_to_timestamp
from .taxi import add_negative_fare_flag, add_likely_reimbursement_flag
from .weather import separate_time_to_timestamp
from .zones import add_county_from_borough


def execute_dataspecific_functions(df, config, logger=None):
    for specific_func in config["extra_functions"]:
        if logger is not None and logger.enabled:
            logger.step("dataset transformation", f"Running {specific_func}")
            before = df.count()
        match specific_func:
            case "add_county_from_borough":
                df = add_county_from_borough(df)
            case "separate_time_to_timestamp":
                df = separate_time_to_timestamp(df)
            case "dates_and_times_to_timestamp":
                df = dates_and_times_to_timestamp(df)
            case "add_negative_fare_flag":
                df = add_negative_fare_flag(df)
            case "add_likely_reimbursement_flag":
                df = add_likely_reimbursement_flag(df)
            case _:
                raise ValueError(f"Unknown dataset transformation: {specific_func}")
        if logger is not None and logger.enabled:
            after = df.count()
            logger.removed(
                f"rows rejected by {specific_func}",
                before - after,
                before,
                after,
            )
    return df
