#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  3 20:25:14 2022

@author: admin
"""

from os.path import abspath
from pyspark.sql import SparkSession

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()


columns = ["id", "name","age","gender"]

# Create DataFrame 
data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]
sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Create temporary view
sampleDF.createOrReplaceTempView("sampleView")

# Create a Database CT
spark.sql("CREATE DATABASE IF NOT EXISTS ct")

# Create a Table naming as sampleTable under CT database.
spark.sql("CREATE TABLE ct.sampleTable (id Int, name String, age Int, gender String)")

# Insert into sampleTable using the sampleView. 
spark.sql("INSERT INTO TABLE ct.sampleTable  SELECT * FROM sampleView")

# Lets view the data in the table
spark.sql("SELECT * FROM ct.sampleTable").show()
