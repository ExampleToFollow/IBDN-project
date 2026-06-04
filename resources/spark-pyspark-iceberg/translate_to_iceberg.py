#!/usr/bin/env python

import sys, os
from os import environ

def main():

  APP_NAME = "translate_to_iceberg.py"

  import pyspark
  import pyspark.sql

  spark = (
    pyspark.sql.SparkSession.builder
    .appName(APP_NAME)
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg apunta al flights
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "s3a://flights")
    .getOrCreate()
  )

  sc = spark.sparkContext

  from pyspark.sql.types import (
    StringType, IntegerType, DoubleType,
    DateType, TimestampType,
    StructType, StructField
  )

  schema = StructType([
    StructField("ArrDelay",   DoubleType(),    True),
    StructField("CRSArrTime", TimestampType(), True),
    StructField("CRSDepTime", TimestampType(), True),
    StructField("Carrier",    StringType(),    True),
    StructField("DayOfMonth", IntegerType(),   True),
    StructField("DayOfWeek",  IntegerType(),   True),
    StructField("DayOfYear",  IntegerType(),   True),
    StructField("DepDelay",   DoubleType(),    True),
    StructField("Dest",       StringType(),    True),
    StructField("Distance",   DoubleType(),    True),
    StructField("FlightDate", DateType(),      True),
    StructField("FlightNum",  StringType(),    True),
    StructField("Origin",     StringType(),    True),
  ])

  # Lee el comprimido original desde raw 
  input_path = "s3a://raw/simple_flight_delay_features.jsonl.bz2"
  print("Reading dataset from: {}".format(input_path))
  features = spark.read.json(input_path, schema=schema)

  print("Row count: {}".format(features.count()))
  features.printSchema()
  features.show(5)

  #  flight_features quedará en s3a://flights/flight_features/
  spark.sql("CREATE NAMESPACE IF NOT EXISTS local.flight_features")

  output_table = "local.flight_features.raw"
  print("Writing Iceberg table: {}".format(output_table))

  (
    features
    .writeTo(output_table)
    .tableProperty("write.format.default", "parquet")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
  )

  print("Iceberg table written successfully.")

  # Verificar
  verify = spark.read.format("iceberg").load("s3a://flights/flight_features/raw")
  print("Verification row count: {}".format(verify.count()))
  verify.show(5)

if __name__ == "__main__":
  main()