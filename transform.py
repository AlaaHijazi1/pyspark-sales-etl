from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from extract import extract


def validate(df: DataFrame):
    df_with_validated_date = df.withColumn(
        "date",
        F.coalesce(
            F.to_date("OrderDate", "yyyy-MM-dd"),
            F.to_date("OrderDate", "M/d/yyyy"),
        ),
    )

    is_valid = (
        F.col("OrderID").isNotNull()
        & (F.trim(F.col("OrderID")) != "")
        & (F.col("Quantity") > 0)
        & (F.col("UnitPrice") > 0)
        & F.col("date").isNotNull()
    )
    is_valid = F.coalesce(is_valid, F.lit(False))

    valid = df_with_validated_date.filter(is_valid)
    valid = valid.withColumn("OrderDate", F.col("date")).drop("date")
    rejected_records = df_with_validated_date.filter(~is_valid).drop("date")
    return valid, rejected_records


def transform(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
        .withColumn("Year", F.year("OrderDate"))
        .withColumn("Month", F.month("OrderDate"))
        .dropDuplicates()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("transform")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = extract(spark)
        valid, rejected_records = validate(df)

        print("Summary report :")
        print(f"Valid records   : {valid.count()}")
        print(f"Invalid records : {rejected_records.count()}")

        transformed_df = transform(valid)
        transformed_df.show(5)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
