from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import great_expectations as gx
import pandas as pd

from validation.config import (
    ANOMALY_IQR_MULTIPLIER,
    CRITICAL_COLUMNS,
    EXPECTED_COLUMNS,
    MAX_DISCOUNT_PCT,
    MIN_DISCOUNT_PCT,
    MIN_QUANTITY,
    MIN_UNIT_PRICE,
    SALES_AMOUNT_TOLERANCE,
    VALID_CATEGORIES,
    VALID_PAYMENT_TYPES,
    VALID_REGIONS,
    VALID_SALES_CHANNELS,
)
from validation.utils import (
    calculate_expected_sales_amount,
    get_iqr_bounds,
    safe_parse_dates,
)

@dataclass
class ValidationSummary:
    dataset_level_failures: list[dict[str, Any]]
    row_level_failures: dict[str, list[int]]
    row_level_warnings: dict[str, list[int]]

    def has_critical_failures(self) -> bool:
        return bool(self.dataset_level_failures) or any(self.row_level_failures.values())

def run_gx_expectations(df: pd.DataFrame) -> list[dict[str, Any]]:
    context = gx.get_context()

    data_source_name = "dqf_pandas_source"
    data_asset_name = "sales_daily_asset"
    batch_definition_name = "sales_daily_batch"

    # Reuse existing objects if they already exist in the context.
    try:
        data_source = context.data_sources.get(data_source_name)
    except Exception:
        data_source = context.data_sources.add_pandas(name=data_source_name)

    try:
        data_asset = data_source.get_asset(data_asset_name)
    except Exception:
        data_asset = data_source.add_dataframe_asset(name=data_asset_name)

    try:
        batch_definition = data_asset.get_batch_definition(batch_definition_name)
    except Exception:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            batch_definition_name
        )

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    expectation_checks = [
        (
            "schema_columns_match",
            gx.expectations.ExpectTableColumnsToMatchOrderedList(
                column_list=EXPECTED_COLUMNS
            ),
        ),
        (
            "order_id_not_null",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"),
        ),
        (
            "customer_id_not_null",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
        ),
        (
            "product_id_not_null",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"),
        ),
        (
            "category_not_null",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="category"),
        ),
        (
            "quantity_not_null",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="quantity"),
        ),
        (
            "unit_price_not_null",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="unit_price"),
        ),
        (
            "sales_amount_not_null",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="sales_amount"),
        ),
        (
            "discount_pct_not_null",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="discount_pct"),
        ),
        (
            "order_id_unique",
            gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"),
        ),
        (
            "category_domain",
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="category",
                value_set=VALID_CATEGORIES,
            ),
        ),
        (
            "region_domain",
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="region",
                value_set=VALID_REGIONS,
            ),
        ),
        (
            "sales_channel_domain",
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="sales_channel",
                value_set=VALID_SALES_CHANNELS,
            ),
        ),
        (
            "payment_type_domain",
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="payment_type",
                value_set=VALID_PAYMENT_TYPES,
            ),
        ),
        (
            "quantity_min_value",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="quantity",
                min_value=MIN_QUANTITY,
                max_value=None,
            ),
        ),
        (
            "unit_price_positive",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="unit_price",
                min_value=MIN_UNIT_PRICE,
                max_value=None,
            ),
        ),
        (
            "discount_pct_range",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="discount_pct",
                min_value=MIN_DISCOUNT_PCT,
                max_value=MAX_DISCOUNT_PCT,
            ),
        ),
    ]

    results: list[dict[str, Any]] = []

    for rule_name, expectation in expectation_checks:
        validation_result = batch.validate(expectation)

        if not validation_result.success:
            results.append(
                {
                    "rule": rule_name,
                    "success": False,
                    "result": validation_result.describe(),
                }
            )

    return results
    gx_df = gx.from_pandas(df)
    results: list[dict[str, Any]] = []

    expectation_checks = [
        (
            "schema_columns_match",
            gx_df.expect_table_columns_to_match_ordered_list(EXPECTED_COLUMNS),
        ),
        (
            "order_id_not_null",
            gx_df.expect_column_values_to_not_be_null("order_id"),
        ),
        (
            "customer_id_not_null",
            gx_df.expect_column_values_to_not_be_null("customer_id"),
        ),
        (
            "product_id_not_null",
            gx_df.expect_column_values_to_not_be_null("product_id"),
        ),
        (
            "category_not_null",
            gx_df.expect_column_values_to_not_be_null("category"),
        ),
        (
            "quantity_not_null",
            gx_df.expect_column_values_to_not_be_null("quantity"),
        ),
        (
            "unit_price_not_null",
            gx_df.expect_column_values_to_not_be_null("unit_price"),
        ),
        (
            "sales_amount_not_null",
            gx_df.expect_column_values_to_not_be_null("sales_amount"),
        ),
        (
            "discount_pct_not_null",
            gx_df.expect_column_values_to_not_be_null("discount_pct"),
        ),
        (
            "order_id_unique",
            gx_df.expect_column_values_to_be_unique("order_id"),
        ),
        (
            "category_domain",
            gx_df.expect_column_values_to_be_in_set("category", VALID_CATEGORIES),
        ),
        (
            "region_domain",
            gx_df.expect_column_values_to_be_in_set("region", VALID_REGIONS),
        ),
        (
            "sales_channel_domain",
            gx_df.expect_column_values_to_be_in_set("sales_channel", VALID_SALES_CHANNELS),
        ),
        (
            "payment_type_domain",
            gx_df.expect_column_values_to_be_in_set("payment_type", VALID_PAYMENT_TYPES),
        ),
        (
            "quantity_min_value",
            gx_df.expect_column_values_to_be_between(
                "quantity",
                min_value=MIN_QUANTITY,
                max_value=None,
            ),
        ),
        (
            "unit_price_positive",
            gx_df.expect_column_values_to_be_between(
                "unit_price",
                min_value=MIN_UNIT_PRICE,
                max_value=None,
            ),
        ),
        (
            "discount_pct_range",
            gx_df.expect_column_values_to_be_between(
                "discount_pct",
                min_value=MIN_DISCOUNT_PCT,
                max_value=MAX_DISCOUNT_PCT,
            ),
        ),
    ]

    for rule_name, result in expectation_checks:
        if not result.success:
            results.append(
                {
                    "rule": rule_name,
                    "success": False,
                    "result": result.to_json_dict(),
                }
            )

    return results

def validate_schema(df: pd.DataFrame) -> list[int]:
    if list(df.columns) != EXPECTED_COLUMNS:
        return list(df.index)
    return []

def validate_missing_critical_values(df: pd.DataFrame) -> list[int]:
    invalid_mask = df[CRITICAL_COLUMNS].isnull().any(axis=1)
    return df.index[invalid_mask].tolist()

def validate_duplicate_order_ids(df: pd.DataFrame) -> list[int]:
    duplicate_mask = df.duplicated(subset=["order_id"], keep=False)
    return df.index[duplicate_mask].tolist()

def validate_date_format(df: pd.DataFrame) -> list[int]:
    parsed_dates = safe_parse_dates(df["order_date"])
    invalid_mask = parsed_dates.isna()
    return df.index[invalid_mask].tolist()

def validate_numeric_ranges(df: pd.DataFrame) -> dict[str, list[int]]:
    results: dict[str, list[int]] = {}

    quantity_invalid = pd.to_numeric(df["quantity"], errors="coerce") < MIN_QUANTITY
    unit_price_invalid = pd.to_numeric(df["unit_price"], errors="coerce") < MIN_UNIT_PRICE
    discount_invalid = (
        (pd.to_numeric(df["discount_pct"], errors="coerce") < MIN_DISCOUNT_PCT)
        | (pd.to_numeric(df["discount_pct"], errors="coerce") > MAX_DISCOUNT_PCT)
    )

    results["quantity_range_invalid"] = df.index[quantity_invalid.fillna(False)].tolist()
    results["unit_price_range_invalid"] = df.index[unit_price_invalid.fillna(False)].tolist()
    results["discount_pct_range_invalid"] = df.index[discount_invalid.fillna(False)].tolist()

    return results

def validate_sales_amount_consistency(df: pd.DataFrame) -> list[int]:
    quantity = pd.to_numeric(df["quantity"], errors="coerce")
    unit_price = pd.to_numeric(df["unit_price"], errors="coerce")
    discount_pct = pd.to_numeric(df["discount_pct"], errors="coerce")
    sales_amount = pd.to_numeric(df["sales_amount"], errors="coerce")

    expected_amount = calculate_expected_sales_amount(quantity, unit_price, discount_pct)
    diff = (sales_amount - expected_amount).abs()

    invalid_mask = diff > SALES_AMOUNT_TOLERANCE
    invalid_mask = invalid_mask.fillna(False)

    return df.index[invalid_mask].tolist()

def detect_anomalies(df: pd.DataFrame) -> dict[str, list[int]]:
    warnings: dict[str, list[int]] = {}

    unit_price = pd.to_numeric(df["unit_price"], errors="coerce")
    sales_amount = pd.to_numeric(df["sales_amount"], errors="coerce")

    unit_price_lower, unit_price_upper = get_iqr_bounds(unit_price, ANOMALY_IQR_MULTIPLIER)
    sales_amount_lower, sales_amount_upper = get_iqr_bounds(sales_amount, ANOMALY_IQR_MULTIPLIER)

    unit_price_outlier_mask = (unit_price < unit_price_lower) | (unit_price > unit_price_upper)
    sales_amount_outlier_mask = (sales_amount < sales_amount_lower) | (sales_amount > sales_amount_upper)

    warnings["unit_price_outliers"] = df.index[unit_price_outlier_mask.fillna(False)].tolist()
    warnings["sales_amount_outliers"] = df.index[sales_amount_outlier_mask.fillna(False)].tolist()

    return warnings

def run_validation_checks(df: pd.DataFrame) -> ValidationSummary:
    dataset_level_failures = run_gx_expectations(df)

    row_level_failures: dict[str, list[int]] = {
        "schema_mismatch": validate_schema(df),
        "missing_critical_values": validate_missing_critical_values(df),
        "duplicate_order_ids": validate_duplicate_order_ids(df),
        "invalid_order_date": validate_date_format(df),
        "sales_amount_inconsistency": validate_sales_amount_consistency(df),
    }

    numeric_range_results = validate_numeric_ranges(df)
    row_level_failures.update(numeric_range_results)

    row_level_warnings = detect_anomalies(df)

    return ValidationSummary(
        dataset_level_failures=dataset_level_failures,
        row_level_failures=row_level_failures,
        row_level_warnings=row_level_warnings,
    )