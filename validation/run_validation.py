from __future__ import annotations

from pathlib import Path

import pandas as pd

from validation.expectations.sales_expectations import run_validation_checks


INPUT_PATH = Path("data/raw/sales_daily.csv")


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


def main() -> None:
    df = load_dataset()
    summary = run_validation_checks(df)
    print_validation_summary(summary)


if __name__ == "__main__":
    main()