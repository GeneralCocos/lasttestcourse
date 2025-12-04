import org.apache.spark.sql.SparkSession

object PgToIcebergScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PgToIcebergScala")
      .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakehouse.type", "hive")
      .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
      .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg")
      .getOrCreate()

    val pgDf = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://postgres:5432/demo")
      .option("dbtable", "public.source_table")
      .option("user", "demo")
      .option("password", "demo")
      .option("driver", "org.postgresql.Driver")
      .load()

    pgDf.createOrReplaceTempView("pg_source_scala")

    spark.sql(
      """
        |CREATE DATABASE IF NOT EXISTS lakehouse_demo
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS lakehouse.lakehouse_demo.pg_iceberg_scala
        |USING iceberg
        |AS SELECT * FROM pg_source_scala
        |""".stripMargin)

    spark.stop()
  }
}
