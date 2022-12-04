#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  3 22:14:59 2022

@author: admin
"""

from pyspark.sql import SparkSession

# Create spark session
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .enableHiveSupport() \
    .getOrCreate()
    
# Read hive table using table()
df = spark.read.table("employee")
df.show()

df = spark.sql("select * from employee")
df.show()