from pyspark.sql import SparkSession


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

    pg_df.createOrReplaceTempView("pg_source")

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
