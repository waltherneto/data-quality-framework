# [DQF-GEN-001]
from __future__ import annotations

import random
from pathlib import Path

import numpy as np
import pandas as pd


RANDOM_SEED = 42
ROW_COUNT = 5000

OUTPUT_PATH = Path("data/raw/sales_daily.csv")

CATEGORIES = ["Electronics", "Home", "Accessories", "Toys", "Office"]
REGIONS = ["North", "South", "Southeast", "Midwest", "Northeast"]
SALES_CHANNELS = ["Online", "Store", "Marketplace"]
PAYMENT_TYPES = ["Credit Card", "Debit Card", "Pix", "Boleto"]

def set_seed(seed: int = RANDOM_SEED) -> None:
    random.seed(seed)
    np.random.seed(seed)

def generate_base_dataset(row_count: int = ROW_COUNT) -> pd.DataFrame:
    date_range = pd.date_range(start="2026-01-01", end="2026-03-01", freq="D")

    records = []

    for i in range(1, row_count + 1):
        order_id = f"O{i:06d}"
        order_date = pd.Timestamp(np.random.choice(date_range)).strftime("%Y-%m-%d")
        customer_id = f"C{np.random.randint(1, 1501):04d}"
        product_id = f"P{np.random.randint(1, 401):04d}"
        category = random.choice(CATEGORIES)
        region = random.choice(REGIONS)
        sales_channel = random.choice(SALES_CHANNELS)
        quantity = np.random.randint(1, 6)
        unit_price = round(np.random.uniform(10, 1500), 2)
        discount_pct = round(np.random.choice([0, 0.05, 0.10, 0.15, 0.20]), 2)
        payment_type = random.choice(PAYMENT_TYPES)

        gross_amount = quantity * unit_price
        sales_amount = round(gross_amount * (1 - discount_pct), 2)

        records.append(
            {
                "order_id": order_id,
                "order_date": order_date,
                "customer_id": customer_id,
                "product_id": product_id,
                "category": category,
                "region": region,
                "sales_channel": sales_channel,
                "quantity": quantity,
                "unit_price": unit_price,
                "sales_amount": sales_amount,
                "payment_type": payment_type,
                "discount_pct": discount_pct,
            }
        )

    return pd.DataFrame(records)

def inject_missing_values(df: pd.DataFrame, frac: float = 0.015) -> pd.DataFrame:
    df = df.copy()
    n_rows = max(1, int(len(df) * frac))
    candidate_columns = ["customer_id", "quantity", "unit_price", "category"]

    sampled_indices = np.random.choice(df.index, size=n_rows, replace=False)

    for idx in sampled_indices:
        col = random.choice(candidate_columns)
        df.at[idx, col] = np.nan

    return df


def inject_duplicates(df: pd.DataFrame, frac: float = 0.01) -> pd.DataFrame:
    df = df.copy()
    n_rows = max(1, int(len(df) * frac))

    duplicate_rows = df.sample(n=n_rows, random_state=RANDOM_SEED)
    df = pd.concat([df, duplicate_rows], ignore_index=True)

    return df


def inject_invalid_ranges(df: pd.DataFrame, frac: float = 0.015) -> pd.DataFrame:
    df = df.copy()
    n_rows = max(1, int(len(df) * frac))
    sampled_indices = np.random.choice(df.index, size=n_rows, replace=False)

    for idx in sampled_indices:
        invalid_case = random.choice(
            ["quantity_zero", "quantity_negative", "unit_price_zero", "discount_negative", "discount_over_limit"]
        )

        if invalid_case == "quantity_zero":
            df.at[idx, "quantity"] = 0
        elif invalid_case == "quantity_negative":
            df.at[idx, "quantity"] = -1
        elif invalid_case == "unit_price_zero":
            df.at[idx, "unit_price"] = 0
        elif invalid_case == "discount_negative":
            df.at[idx, "discount_pct"] = -0.10
        elif invalid_case == "discount_over_limit":
            df.at[idx, "discount_pct"] = 0.75

    return df


def inject_amount_inconsistencies(df: pd.DataFrame, frac: float = 0.01) -> pd.DataFrame:
    df = df.copy()
    n_rows = max(1, int(len(df) * frac))
    sampled_indices = np.random.choice(df.index, size=n_rows, replace=False)

    for idx in sampled_indices:
        current_amount = df.at[idx, "sales_amount"]
        df.at[idx, "sales_amount"] = round(float(current_amount) * np.random.uniform(1.2, 1.8), 2)

    return df


def inject_bad_dates(df: pd.DataFrame, frac: float = 0.005) -> pd.DataFrame:
    df = df.copy()
    n_rows = max(1, int(len(df) * frac))
    sampled_indices = np.random.choice(df.index, size=n_rows, replace=False)

    bad_date_values = ["2026/02/15", "15-02-2026", "invalid_date"]

    for idx in sampled_indices:
        df.at[idx, "order_date"] = random.choice(bad_date_values)

    return df


def inject_anomalies(df: pd.DataFrame, frac: float = 0.005) -> pd.DataFrame:
    df = df.copy()
    n_rows = max(1, int(len(df) * frac))
    sampled_indices = np.random.choice(df.index, size=n_rows, replace=False)

    for idx in sampled_indices:
        df.at[idx, "unit_price"] = round(np.random.uniform(10000, 50000), 2)
        quantity = df.at[idx, "quantity"]

        if pd.notna(quantity) and float(quantity) > 0:
            discount = df.at[idx, "discount_pct"]
            df.at[idx, "sales_amount"] = round(float(quantity) * df.at[idx, "unit_price"] * (1 - float(discount)), 2)

    return df


def build_dataset() -> pd.DataFrame:
    df = generate_base_dataset()
    df = inject_missing_values(df)
    df = inject_invalid_ranges(df)
    df = inject_amount_inconsistencies(df)
    df = inject_bad_dates(df)
    df = inject_anomalies(df)
    df = inject_duplicates(df)

    return df


def save_dataset(df: pd.DataFrame, output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def main() -> None:
    set_seed()
    df = build_dataset()
    save_dataset(df)
    print(f"Dataset generated successfully with {len(df)} rows at: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()