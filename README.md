# Sales ETL Pipeline (PySpark)

A PySpark pipeline that reads raw sales CSV files, validates and transforms them, and stores the result as Parquet and as Parquet partitioned by year and month.

**Run:** `python load.py` (runs the full pipeline).

## ETL flow

```
data/*.csv -> extract -> validate -> transform -> output/sales
                              |                -> output/sales_partitioned
                              +-> rejected_records
```

| File | Responsibility |
|---|---|
| `extract.py` | Reads all CSV files in `data/` with an explicit schema; reports record count and nulls in mandatory fields |
| `transform.py` | Validation, derived columns, duplicate removal; reports valid/invalid counts |
| `load.py` | Writes both Parquet datasets; compares file sizes and query times |

## Validation rules

1. `OrderID` is not null or blank.
2. `Quantity` > 0.
3. `UnitPrice` > 0.
4. `OrderDate` is a valid date (`yyyy-MM-dd` or `M/d/yyyy`).

Rows failing any rule are separated into the `rejected_records` DataFrame.

| Total | Valid | Rejected |
|---|---|---|
| 35 | 31 | 4 |

## Transformation logic

- `Revenue = Quantity * UnitPrice`
- `Year = year(OrderDate)`
- `Month = month(OrderDate)`
- Duplicate records are removed with `dropDuplicates()`.

## Partitioning strategy

The cleaned data is partitioned by `Year` and `Month`, the columns most queries filter on. Both have low cardinality, so the number of folders stays small. A query for one month reads only its folder and skips the rest (partition pruning).

```
output/sales_partitioned/
└── Year=<YYYY>/
    └── Month=<M>/
```

## Results

| | CSV (raw) | Parquet (cleaned) |
|---|---|---|
| Size | 1,502 bytes | 3,577 bytes |

Query `Year=2026, Month=3` (11 records, identical on both datasets), median of 5 runs after warm-up:

| Dataset | Time |
|---|---|
| Non-partitioned | 329 ms |
| Partitioned | 283 ms |

## Observed benefits of Parquet and partitioning

- **Parquet:** On this small dataset (35 rows), the Parquet output is larger than the CSV, because each Parquet file carries fixed metadata (schema, column statistics) that outweighs compression on tiny data. Parquet is columnar, compressed and type-preserving, so its size and read-speed advantages appear on large datasets.
- **Partitioning:** The partitioned query was slightly faster (283 ms vs 329 ms), but the difference is small and within normal measurement noise at this size, so it is not conclusive. The expected benefit is that Spark reads only the matching `Year=/Month=` folder, and this grows with data volume.
- **Note:** The CSV size is the raw input, while the Parquet holds cleaned rows plus three extra columns (`Revenue`, `Year`, `Month`).