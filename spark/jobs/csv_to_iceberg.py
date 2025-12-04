from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("CsvToIcebergPython")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "file:/warehouse")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
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

    df.createOrReplaceTempView("csv_source")

    # 1. создаём namespace именно в каталоге lakehouse (Iceberg + Hive)
    # это создаст базу lakehouse_demo в том же Hive Metastore, с file:// локацией
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
