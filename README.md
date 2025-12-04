# Data Lakehouse Demo

This project demonstrates end-to-end ingestion of wide CSV and Postgres tables into S3-backed Iceberg tables using both PySpark and Scala Spark jobs.

## Prerequisites
- Docker and Docker Compose installed

## One-time setup
1. Generate a sample CSV with 50 columns and 1,000 rows:
   ```bash
   python data/generate_csv.py
   ```

## How to run the stack
1. Start the core services (Postgres, MinIO, Hive Metastore, Spark master/worker):
   ```bash
   docker-compose up -d
   ```
   The MinIO service now has a healthcheck; the bucket-initializer waits until MinIO is ready before creating the `landing` and `warehouse` buckets.

2. Verify all containers are healthy before running Spark jobs:
   ```bash
   docker-compose ps
   ```

## Run the Spark ingestion jobs
All commands below run inside the `spark-master` container.

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
  --class CsvToIceberg \
  /opt/spark-apps/ScalaIcebergJobs.jar
```

### Postgres -> Iceberg (Scala)
```bash
docker-compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --class PgToIceberg \
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
