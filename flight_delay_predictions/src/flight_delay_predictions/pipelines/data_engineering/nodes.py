from typing import Any, Dict

import pandas as pd


def partition_by_month(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """[summary]

    Args:
        df (pd.DataFrame): [description]

    Returns:
        Dict[str, pd.DataFrame]: [description]
    """
    parts = {}

    for month in df["DAY_OF_MONTH"].unique():
        parts[f"DAY_OF_MONTH=={month:02}"] = df[df["DAY_OF_MONTH"] == month]

    return parts


def partition_calc(partitions: Dict[str, pd.DataFrame]) -> Dict[str, str]:
    """[summary]

    Args:
        partitions (Dict[str, pd.DataFrame]): [description]

    Returns:
        Dict[str, str]: [description]
    """
    parts = {}

    for part_key, df in partitions.items():
        print(f"Calc: {part_key}")
        parts[part_key] = str(len(df))

    return parts
