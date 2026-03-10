# Data Quality Framework

A small Data Engineering portfolio project that demonstrates how to implement an automated data validation layer before loading sales data into a warehouse.

## Project Goal

This project simulates a company receiving daily sales data with frequent quality issues such as:
- missing values
- duplicate records
- schema inconsistencies
- invalid numeric ranges
- anomalous values

The objective is to validate incoming data, generate a quality report, and separate clean records from invalid ones before downstream loading.

## Tech Stack

- Python
- Pandas
- Great Expectations

## Repository Structure

```text
data-quality-framework/
│
├── data/
│   ├── raw/
│   ├── clean/
│   └── quarantine/
├── validation/
│   └── expectations/
├── reports/
├── requirements.txt
└── README.md
```

## Status

Initial repository scaffold in progress.