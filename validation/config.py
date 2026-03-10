from __future__ import annotations

EXPECTED_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "category",
    "region",
    "sales_channel",
    "quantity",
    "unit_price",
    "sales_amount",
    "payment_type",
    "discount_pct",
]

CRITICAL_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "sales_amount",
    "discount_pct",
]

VALID_CATEGORIES = ["Electronics", "Home", "Accessories", "Toys", "Office"]
VALID_REGIONS = ["North", "South", "Southeast", "Midwest", "Northeast"]
VALID_SALES_CHANNELS = ["Online", "Store", "Marketplace"]
VALID_PAYMENT_TYPES = ["Credit Card", "Debit Card", "Pix", "Boleto"]

MIN_DISCOUNT_PCT = 0.0
MAX_DISCOUNT_PCT = 0.50
MIN_QUANTITY = 1
MIN_UNIT_PRICE = 0.01
SALES_AMOUNT_TOLERANCE = 0.01

ANOMALY_IQR_MULTIPLIER = 1.5