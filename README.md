# Sales ETL Pipeline (PySpark)

A PySpark pipeline that reads raw sales CSV files, validates and transforms them, and stores the cleaned result as Parquet and as Parquet partitioned by year and month.

**Run:** `python load.py`

## ETL flow

```
data/*.csv --> extract --> validate --> transform --+--> output/sales
                              |                     |
                              |                     +--> output/sales_partitioned
                              |
                              +--> rejected_records
```

| File           | Responsibility                                                                                                              |
| -------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `extract.py`   | Reads all CSV files in `data/` with an explicit schema; reports record count and nulls in mandatory fields                  |
| `transform.py` | Validation, derived columns, duplicate removal; reports valid/invalid counts                                                |
| `load.py`      | Runs the full pipeline; writes both Parquet datasets; compares CSV vs Parquet size and partitioned vs non-partitioned query time |


## Validation rules

1. `OrderID` is not null or blank.
2. `Quantity` > 0.
3. `UnitPrice` > 0.
4. `OrderDate` is a valid date.

Rows failing any rule are separated into the `rejected_records` DataFrame.

| Valid records | Invalid records |
| ----- | -------- |
| 31    | 4        |

## Transformation logic

- `Revenue = Quantity * UnitPrice`
- `Year = year(OrderDate)`
- `Month = month(OrderDate)`
- Duplicate records are removed with `dropDuplicates()`.

## Partitioning strategy

The cleaned data is partitioned by `Year` and `Month`, the columns commonly used to filter sales data. Spark can skip irrelevant partitions through **partition pruning**, reducing the amount of data read.

```text
sales_partitioned/
└── Year=2026/
    └── Month=3/
```

## Observed benefits of Parquet and partitioning

### Parquet

| | CSV (raw) | Parquet (cleaned) |
|---|---|---|
| Size | 1,502 bytes | 3,577 bytes |

On this small dataset (35 rows), the Parquet output is larger than the CSV, because each Parquet file carries fixed metadata (schema, column statistics) that outweighs compression on tiny data. Parquet is columnar, compressed and type-preserving, so its size and read-speed advantages appear on large datasets.

The CSV size is the raw input, while the Parquet holds cleaned rows plus three extra columns (`Revenue`, `Year`, `Month`).

### Partitioning

Query `Year=2026, Month=3` (11 records, identical on both datasets), median of 5 runs after warm-up:

| Dataset | Time |
|---|---|
| Non-partitioned | 329 ms |
| Partitioned | 283 ms |

The partitioned query was faster because Spark reads only the matching `Year=2026/Month=3` folder and skips all others (partition pruning), instead of scanning the whole dataset. The difference is small here because the dataset is small; the benefit is expected to grow with data volume, since more data is skipped.