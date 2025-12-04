# Data Lakehouse Demo

This project demonstrates end-to-end ingestion of wide CSV and Postgres tables into S3-backed Iceberg tables using both PySpark and Scala Spark jobs.

## Prerequisites
- Docker and Docker Compose installed

## One-time setup
1. Generate a sample CSV with 50 columns and 1,000 rows:
   ```bash
   python data/generate_csv.py
   ```

2. Build the Hive Metastore image with bundled S3A dependencies (required for Iceberg metadata on MinIO). The build now installs
   `curl` inside the image so the S3A jars download successfully even though the base Hive image does not ship with it:
   ```bash
   docker-compose build hive-metastore
   ```

3. (Optional) If you're running behind a proxy or firewalled network, make sure the Spark containers can download Maven artifacts.
   The stack automatically pulls the Iceberg runtime and Postgres JDBC driver defined in `conf/spark-defaults.conf` during the
   first Spark submission. If external downloads are blocked, pre-populate the jars in the Docker image or mirror them locally.

## How to run the stack
1. Start the core services (Postgres, MinIO, Hive Metastore, Spark master/worker):
   ```bash
   docker-compose up -d
   ```
   The MinIO service now has a healthcheck; the bucket-initializer waits until MinIO is ready before creating the `landing` and `warehouse` buckets.

2. Check that containers are healthy and the Hive Metastore build completed (the image pulls S3A dependencies on first build):
   ```bash
   docker-compose ps
   docker-compose logs -f hive-metastore  # wait for "Starting Hive Metastore Server" and no download errors
   ```

## Run the Spark ingestion jobs
All commands below run inside the `spark-master` container.

The Spark defaults already point at the Iceberg catalog (`lakehouse`) and include the necessary dependencies
(`iceberg-spark-runtime-3.5_2.12` and `postgresql` JDBC driver), so the commands below can run without extra
`--packages` flags.

### CSV -> Iceberg (PySpark)
```bash
docker-compose exec spark-master \
  /opt/spark/bin/spark-submit /opt/spark-apps/csv_to_iceberg.py
```

### Postgres -> Iceberg (PySpark)
```bash
docker-compose exec spark-master \
  /opt/spark/bin/spark-submit /opt/spark-apps/pg_to_iceberg.py
```

### CSV -> Iceberg (Scala)
```bash
docker-compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --class CsvToIcebergScala \
  /opt/spark-apps/ScalaIcebergJobs.jar
```

### Postgres -> Iceberg (Scala)
```bash
docker-compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --class PgToIcebergScala \
  /opt/spark-apps/ScalaIcebergJobs.jar
```

## Inspect Iceberg tables
Open an interactive Spark shell that already points to the Hive-backed Iceberg catalog:
```bash
docker-compose exec spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.hive.HiveCatalog \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.lakehouse.warehouse=s3a://warehouse/iceberg
```

Example verification queries:
```sql
SHOW TABLES IN lakehouse.lakehouse_demo;
SELECT COUNT(*) FROM lakehouse.lakehouse_demo.csv_iceberg_python;
SELECT COUNT(*) FROM lakehouse.lakehouse_demo.pg_iceberg_python;
```

## Teardown
```bash
docker-compose down -v
```
