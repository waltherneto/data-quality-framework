from __future__ import annotations

from collections import defaultdict

import pandas as pd


def safe_parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def calculate_expected_sales_amount(
    quantity: pd.Series,
    unit_price: pd.Series,
    discount_pct: pd.Series,
) -> pd.Series:
    expected = quantity * unit_price * (1 - discount_pct)
    return expected.round(2)


def get_iqr_bounds(series: pd.Series, multiplier: float = 1.5) -> tuple[float, float]:
    clean_series = series.dropna()
    if clean_series.empty:
        return (float("-inf"), float("inf"))

    q1 = clean_series.quantile(0.25)
    q3 = clean_series.quantile(0.75)
    iqr = q3 - q1

    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    return float(lower_bound), float(upper_bound)


def build_row_rule_map(row_level_results: dict[str, list[int]]) -> dict[int, list[str]]:
    row_rule_map: dict[int, list[str]] = defaultdict(list)

    for rule_name, row_indices in row_level_results.items():
        for row_index in row_indices:
            row_rule_map[row_index].append(rule_name)

    return dict(row_rule_map)


def stringify_rule_list(rule_list: list[str]) -> str:
    return "; ".join(sorted(rule_list))