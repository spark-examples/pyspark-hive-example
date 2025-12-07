# pyspark-hive-example

A collection of small PySpark scripts that demonstrate creating and reading Hive tables.

## Examples
- `src/pyspark-write-hive-table.py`: create a Hive managed table from a DataFrame.
- `src/pyspark-read-hive-table.py`: read a Hive table using both table() and SQL APIs.
- `src/pyspark-save-table-from-temp-view.py`: insert data into a Hive table from a temporary view.
- `src/pyspark-hive-bucketing-example.py`: create a bucketed Hive table and inspect its metadata.

Each script expects a Spark installation with Hive support enabled. Run them with `spark-submit` or by invoking the files directly if your environment is configured for PySpark.
