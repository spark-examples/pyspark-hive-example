#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Example: write and query a partitioned Hive table with PySpark.
"""

from os.path import abspath

from pyspark.sql import SparkSession

# Configure the warehouse directory for managed Hive tables.
warehouse_location = abspath("spark-warehouse")

spark = (
    SparkSession.builder.appName("Spark Hive Partition Example")
    .config("spark.sql.warehouse.dir", warehouse_location)
    .enableHiveSupport()
    .getOrCreate()
)

data = [
    ("James", "Sales", 3000, 2019),
    ("Michael", "Sales", 4600, 2020),
    ("Robert", "Sales", 4100, 2020),
    ("Maria", "Finance", 3000, 2019),
    ("Raman", "Finance", 3000, 2019),
    ("Scott", "Finance", 3300, 2021),
]

columns = ["employee_name", "department", "salary", "year"]

df = spark.createDataFrame(data, columns)

# Create a database if it does not exist.
spark.sql("CREATE DATABASE IF NOT EXISTS reporting")

# Write the DataFrame to a managed Hive table partitioned by year.
(
    df.write.mode("overwrite")
    .partitionBy("year")
    .format("parquet")
    .saveAsTable("reporting.employee_salary_by_year")
)

# Show the available partitions.
spark.sql("SHOW PARTITIONS reporting.employee_salary_by_year").show(truncate=False)

# Query the partitioned table and order the results.
(
    spark.table("reporting.employee_salary_by_year")
    .orderBy("year", "department", "employee_name")
    .show(truncate=False)
)
