import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, current_timestamp, sha2}

object CsvToIcebergScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CsvToIcebergScala")
      .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakehouse.type", "hive")
      .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
      .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/opt/data/input.csv")

    val transformed = df
      .withColumn("ingestion_ts", current_timestamp())
      .withColumn("row_hash", sha2(concat_ws("||", df.columns.map(col): _*), 256))

    transformed.createOrReplaceTempView("csv_source_scala")

    spark.sql(
      """
        |CREATE DATABASE IF NOT EXISTS lakehouse_demo
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS lakehouse.lakehouse_demo.csv_iceberg_scala
        |USING iceberg
        |AS SELECT * FROM csv_source_scala
        |""".stripMargin)

    spark.stop()
  }
}
