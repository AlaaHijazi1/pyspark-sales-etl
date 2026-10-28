from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DATA_DIR = Path(__file__).resolve().parent / "data"

SCHEMA = StructType(
    [
        StructField("OrderID", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("ProductID", StringType(), True),
        StructField("OrderDate", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("Country", StringType(), True),
    ]
)

MANDATORY_FIELDS = ["OrderID", "OrderDate", "Quantity", "UnitPrice"]


def extract(spark: SparkSession, data_dir: Path = DATA_DIR) -> DataFrame:
    return spark.read.option("header", True).schema(SCHEMA).csv(str(data_dir / "*.csv"))


def main() -> None:
    spark = (
        SparkSession.builder.appName("extract")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = extract(spark)

        print("Record count:", df.count())

        null_counts = []
        for column in MANDATORY_FIELDS:
            count_nulls = F.sum(F.col(column).isNull().cast("int")).alias(column)
            null_counts.append(count_nulls)
        df.select(null_counts).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
