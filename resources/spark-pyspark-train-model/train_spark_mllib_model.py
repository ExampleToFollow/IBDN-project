#!/usr/bin/env python

import sys, os, re
from os import environ
from datetime import datetime

def main():

  APP_NAME = "train_spark_mllib_model.py"

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
    # Iceberg — apunta al bucket flights
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "s3a://flights")
    .getOrCreate()
  )

  sc = spark.sparkContext

  from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
  from pyspark.sql.types import StructType, StructField
  from pyspark.sql.functions import udf, col, count, when, lit, concat

  # ── Rutas ────────────────────────────────────────────────────────────────
  # Datos — tabla Iceberg en bucket flights
  INPUT_PATH = "s3a://flights/flight_features/raw"

  # Modelos — bucket models
  # production/ → lo que usa el predictor en este momento
  # registry/   → histórico de todos los entrenamientos
  run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  PRODUCTION_PATH = "s3a://models/production"
  REGISTRY_PATH   = "s3a://models/registry/run_{}".format(run_timestamp)

  print("Run timestamp: {}".format(run_timestamp))
  print("Input path:    {}".format(INPUT_PATH))
  print("Registry path: {}".format(REGISTRY_PATH))
  print("Production path: {}".format(PRODUCTION_PATH))

  # ── Lectura desde Iceberg ─────────────────────────────────────────────────
  features = spark.read.format("iceberg").load(INPUT_PATH)

  features = features.repartition(8)
  features.cache()
  features.first()

  # ── Null check en un solo job ─────────────────────────────────────────────
  null_counts = features.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in features.columns
  ]).collect()
  print("Null counts: {}".format(null_counts))

  # ── Route feature ─────────────────────────────────────────────────────────
  features_with_route = features.withColumn(
    'Route',
    concat(features.Origin, lit('-'), features.Dest)
  )
  features_with_route.show(6)

  # ── Bucketizer ────────────────────────────────────────────────────────────
  from pyspark.ml.feature import Bucketizer

  splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
  arrival_bucketizer = Bucketizer(
    splits=splits,
    inputCol="ArrDelay",
    outputCol="ArrDelayBucket"
  )

  # Guardar en registry y en production
  for base_path in [REGISTRY_PATH, PRODUCTION_PATH]:
    arrival_bucketizer.write().overwrite().save(
      "{}/arrival_bucketizer_2.0.bin".format(base_path)
    )

  ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
  ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

  # ── StringIndexers ────────────────────────────────────────────────────────
  from pyspark.ml.feature import StringIndexer, VectorAssembler

  ml_bucketized_features.cache()

  for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer = StringIndexer(
      inputCol=column,
      outputCol=column + "_index"
    )

    string_indexer_model = string_indexer.fit(ml_bucketized_features)
    ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
    ml_bucketized_features = ml_bucketized_features.drop(column)

    # Guardar en registry y en production
    for base_path in [REGISTRY_PATH, PRODUCTION_PATH]:
      string_indexer_model.write().overwrite().save(
        "{}/string_indexer_model_{}.bin".format(base_path, column)
      )

  # ── VectorAssembler ───────────────────────────────────────────────────────
  numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
  index_columns   = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]

  vector_assembler = VectorAssembler(
    inputCols=numeric_columns + index_columns,
    outputCol="Features_vec"
  )
  final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

  # Guardar en registry y en production
  for base_path in [REGISTRY_PATH, PRODUCTION_PATH]:
    vector_assembler.write().overwrite().save(
      "{}/numeric_vector_assembler.bin".format(base_path)
    )

  for column in index_columns:
    final_vectorized_features = final_vectorized_features.drop(column)

  final_vectorized_features.show()
  features.unpersist()

  # ── RandomForest ──────────────────────────────────────────────────────────
  from pyspark.ml.classification import RandomForestClassifier

  rfc = RandomForestClassifier(
    featuresCol="Features_vec",
    labelCol="ArrDelayBucket",
    predictionCol="Prediction",
    maxBins=4657,
    maxMemoryInMB=1024
  )
  model = rfc.fit(final_vectorized_features)

  # Guardar en registry y en production
  for base_path in [REGISTRY_PATH, PRODUCTION_PATH]:
    model.write().overwrite().save(
      "{}/spark_random_forest_classifier.flight_delays.5.0.bin".format(base_path)
    )

  ml_bucketized_features.unpersist()

  # ── Evaluación ────────────────────────────────────────────────────────────
  predictions = model.transform(final_vectorized_features)

  from pyspark.ml.evaluation import MulticlassClassificationEvaluator
  evaluator = MulticlassClassificationEvaluator(
    predictionCol="Prediction",
    labelCol="ArrDelayBucket",
    metricName="accuracy"
  )
  accuracy = evaluator.evaluate(predictions)
  print("Accuracy = {}".format(accuracy))
  print("Run {} saved to registry and promoted to production".format(run_timestamp))

  predictions.groupBy("Prediction").count().show()
  predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)

if __name__ == "__main__":
  main()