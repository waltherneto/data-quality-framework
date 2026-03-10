from __future__ import annotations

from pathlib import Path

import pandas as pd

from validation.expectations.sales_expectations import run_validation_checks
from validation.utils import build_row_rule_map, stringify_rule_list


INPUT_PATH = Path("data/raw/sales_daily.csv")
CLEAN_OUTPUT_PATH = Path("data/clean/sales_daily_clean.csv")
QUARANTINE_OUTPUT_PATH = Path("data/quarantine/sales_daily_invalid.csv")


def load_dataset(input_path: Path = INPUT_PATH) -> pd.DataFrame:
    return pd.read_csv(input_path)


def print_validation_summary(summary) -> None:
    print("\n=== DATA QUALITY VALIDATION SUMMARY ===")

    print("\nDataset-level failures:")
    if not summary.dataset_level_failures:
        print("  - None")
    else:
        for item in summary.dataset_level_failures:
            print(f"  - {item['rule']}")

    print("\nRow-level critical failures:")
    for rule_name, indices in summary.row_level_failures.items():
        print(f"  - {rule_name}: {len(indices)} rows")

    print("\nRow-level warnings:")
    for rule_name, indices in summary.row_level_warnings.items():
        print(f"  - {rule_name}: {len(indices)} rows")


def split_valid_and_invalid_rows(
    df: pd.DataFrame,
    summary,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    row_rule_map = build_row_rule_map(summary.row_level_failures)
    invalid_indices = sorted(row_rule_map.keys())

    invalid_df = df.loc[invalid_indices].copy() if invalid_indices else df.iloc[0:0].copy()
    valid_df = df.drop(index=invalid_indices).copy()

    if not invalid_df.empty:
        invalid_df["failed_rules"] = invalid_df.index.map(
            lambda idx: stringify_rule_list(row_rule_map.get(idx, []))
        )

    return valid_df.reset_index(drop=True), invalid_df.reset_index(drop=True)


def save_output(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def main() -> None:
    df = load_dataset()
    summary = run_validation_checks(df)

    print_validation_summary(summary)

    clean_df, quarantine_df = split_valid_and_invalid_rows(df, summary)

    save_output(clean_df, CLEAN_OUTPUT_PATH)
    save_output(quarantine_df, QUARANTINE_OUTPUT_PATH)

    print("\nOutput files generated:")
    print(f"  - Clean dataset: {CLEAN_OUTPUT_PATH} ({len(clean_df)} rows)")
    print(f"  - Quarantine dataset: {QUARANTINE_OUTPUT_PATH} ({len(quarantine_df)} rows)")


if __name__ == "__main__":
    main()