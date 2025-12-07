#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Example showing how to create and read a Hive bucketed table with PySpark.
"""

from os.path import abspath
from pyspark.sql import SparkSession

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

columns = ["id", "name", "age", "gender", "country"]
data = [
    (1, "James", 30, "M", "US"),
    (2, "Ann", 40, "F", "US"),
    (3, "Jeff", 41, "M", "CA"),
    (4, "Jennifer", 20, "F", "CA"),
    (5, "Robert", 35, "M", "US"),
    (6, "Jane", 28, "F", "US"),
]

df = spark.sparkContext.parallelize(data).toDF(columns)

# Clean up any previous run of the bucketed table
spark.sql("DROP TABLE IF EXISTS bucketed_employee")

# Create a bucketed Hive table on the id column and sort by country within each bucket
(
    df.write
    .mode("overwrite")
    .bucketBy(2, "id")
    .sortBy("country")
    .saveAsTable("bucketed_employee")
)

print("Bucketed table created with 2 buckets on id; showing data:")
spark.sql("SELECT * FROM bucketed_employee ORDER BY id").show()

# Show the bucket metadata so it is clear how the table is stored
spark.sql("DESCRIBE FORMATTED bucketed_employee").show(truncate=False)
