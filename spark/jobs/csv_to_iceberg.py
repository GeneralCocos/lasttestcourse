from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("CsvToIcebergPython")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg")
        .enableHiveSupport()
        .getOrCreate()
    )

    # читаем CSV (лежит в /opt/data внутри контейнера)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/opt/data/input.csv")
    )

    transformed = (
        df.withColumn("ingestion_ts", F.current_timestamp())
        .withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in df.columns]), 256),
        )
    )

    transformed.createOrReplaceTempView("csv_source")

    # 1. создаём namespace именно в каталоге lakehouse (Iceberg + Hive)
    # база lakehouse_demo будет хранить метаданные в Hive, а файлы — в S3 (MinIO)
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS lakehouse.lakehouse_demo
    """)

    # 2. CTAS → Iceberg-таблица в этом namespace
    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.lakehouse_demo.csv_iceberg_python
        USING iceberg
        AS
        SELECT * FROM csv_source
    """)

    spark.stop()


if __name__ == "__main__":
    main()
