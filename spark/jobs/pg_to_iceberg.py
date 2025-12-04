from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def sum_numeric_columns(df: DataFrame):
    numeric_cols = []
    for c, dtype in df.dtypes:
        normalized = dtype.lower()
        if normalized in {"int", "bigint", "double", "float", "long"} or normalized.startswith(
            "decimal"
        ):
            numeric_cols.append(F.col(c).cast("double"))
    if not numeric_cols:
        return F.lit(None)
    expr = numeric_cols[0]
    for col_expr in numeric_cols[1:]:
        expr = expr + col_expr
    return expr


def main():
    spark = (
        SparkSession.builder
        .appName("PgToIcebergPython")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg")
        .getOrCreate()
    )

    pg_df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/demo")
        .option("dbtable", "public.source_table")
        .option("user", "demo")
        .option("password", "demo")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    transformed = (
        pg_df.withColumn("load_date", F.current_date())
        .withColumn("numeric_sum", sum_numeric_columns(pg_df))
    )

    transformed.createOrReplaceTempView("pg_source")

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS lakehouse_demo
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.lakehouse_demo.pg_iceberg_python
        USING iceberg
        AS SELECT * FROM pg_source
    """)

    spark.stop()


if __name__ == "__main__":
    main()
