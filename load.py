import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from extract import extract
from transform import validate, transform

BASE = Path(__file__).resolve().parent
CSV_DIR = BASE / "data"
OUT_SALES = BASE / "output" / "sales"
OUT_PARTITIONED = BASE / "output" / "sales_partitioned"


def size_of(files) -> int:
    return sum(f.stat().st_size for f in files)


def measure_query(spark, path: Path, year: int, month: int, runs: int = 5):
    durations = []
    for run_number in range(runs + 1):
        start_time = time.perf_counter()
        rows = (
            spark.read.parquet(path.as_uri())
            .filter((F.col("Year") == year) & (F.col("Month") == month))
            .collect()
        )
        if run_number > 0:
            durations.append(time.perf_counter() - start_time)
    return statistics.median(durations), rows


def main() -> None:
    spark = (
        SparkSession.builder.appName("load")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        valid, _ = validate(extract(spark))
        clean = transform(valid)

        clean.write.mode("overwrite").parquet(OUT_SALES.as_uri())

        csv_size = size_of(CSV_DIR.glob("*.csv"))
        parquet_size = size_of(OUT_SALES.glob("*.parquet"))

        print("CSV vs Parquet:")
        print(f"CSV size     : {csv_size:,} bytes")
        print(f"Parquet size : {parquet_size:,} bytes")

        (
            clean.write.partitionBy("Year", "Month")
            .mode("overwrite")
            .parquet(OUT_PARTITIONED.as_uri())
        )

        YEAR, MONTH = 2026, 3

        time_unpartitioned, unpartitioned_rows = measure_query(
            spark, OUT_SALES, YEAR, MONTH
        )
        time_partitioned, partitioned_rows = measure_query(
            spark, OUT_PARTITIONED, YEAR, MONTH
        )

        if len(unpartitioned_rows) == 0:
            raise ValueError(f"No data for Year={YEAR}, Month={MONTH}")
        if len(unpartitioned_rows) != len(partitioned_rows):
            raise ValueError("Results differ between the two datasets")

        print(f"\nQuery: Year={YEAR}, Month={MONTH}")
        print(f"Records retrieved: {len(unpartitioned_rows)}")
        print("Sample records:")
        for row in unpartitioned_rows[:3]:
            print(" ", row.asDict())
        print(f"Non-partitioned : {time_unpartitioned * 1000:.0f} ms")
        print(f"Partitioned     : {time_partitioned * 1000:.0f} ms")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
