#!/usr/bin/env python

import sys, os, re
from os import environ
from datetime import datetime
import mlflow
import mlflow.spark

# ── Credenciales MinIO para MLflow ────────────────────────────────────────────
os.environ["AWS_ACCESS_KEY_ID"]     = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")

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

  # ── Rutas ──────────────────────────────────────────────────────────────────
  INPUT_PATH      = "s3a://flights/flight_features/raw"
  run_timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
  PRODUCTION_PATH = "s3a://models/production"
  REGISTRY_PATH   = "s3a://models/registry/run_{}".format(run_timestamp)

  print("Run timestamp:   {}".format(run_timestamp))
  print("Input path:      {}".format(INPUT_PATH))
  print("Registry path:   {}".format(REGISTRY_PATH))
  print("Production path: {}".format(PRODUCTION_PATH))

  # ── MLflow setup ───────────────────────────────────────────────────────────
  MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
  mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
  mlflow.set_experiment("flight_delay_model")

  with mlflow.start_run() as run:
    print("MLflow run_id: {}".format(run.info.run_id))

    # ── Lectura desde Iceberg ─────────────────────────────────────────────
    features = spark.read.format("iceberg").load(INPUT_PATH)
    input_rows = features.count()
    print("Input rows: {}".format(input_rows))

    features = features.repartition(8)
    features.cache()
    features.first()

    # ── Null check ────────────────────────────────────────────────────────
    null_counts = features.select([
      count(when(col(c).isNull(), c)).alias(c)
      for c in features.columns
    ]).collect()
    print("Null counts: {}".format(null_counts))

    # ── Route feature ─────────────────────────────────────────────────────
    features_with_route = features.withColumn(
      'Route',
      concat(features.Origin, lit('-'), features.Dest)
    )
    features_with_route.show(6)

    # ── Bucketizer ────────────────────────────────────────────────────────
    from pyspark.ml.feature import Bucketizer

    splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
    arrival_bucketizer = Bucketizer(
      splits=splits,
      inputCol="ArrDelay",
      outputCol="ArrDelayBucket"
    )

    for base_path in [REGISTRY_PATH, PRODUCTION_PATH]:
      arrival_bucketizer.write().overwrite().save(
        "{}/arrival_bucketizer_2.0.bin".format(base_path)
      )

    ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
    ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

    # ── StringIndexers ────────────────────────────────────────────────────
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

      for base_path in [REGISTRY_PATH, PRODUCTION_PATH]:
        string_indexer_model.write().overwrite().save(
          "{}/string_indexer_model_{}.bin".format(base_path, column)
        )

    # ── VectorAssembler ───────────────────────────────────────────────────
    numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
    index_columns   = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]

    vector_assembler = VectorAssembler(
      inputCols=numeric_columns + index_columns,
      outputCol="Features_vec"
    )
    final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

    for base_path in [REGISTRY_PATH, PRODUCTION_PATH]:
      vector_assembler.write().overwrite().save(
        "{}/numeric_vector_assembler.bin".format(base_path)
      )

    for column in index_columns:
      final_vectorized_features = final_vectorized_features.drop(column)

    final_vectorized_features.show()
    features.unpersist()

    # ── RandomForest ──────────────────────────────────────────────────────
    from pyspark.ml.classification import RandomForestClassifier

    MAX_BINS        = 4657
    MAX_MEMORY_MB   = 1024

    rfc = RandomForestClassifier(
      featuresCol="Features_vec",
      labelCol="ArrDelayBucket",
      predictionCol="Prediction",
      maxBins=MAX_BINS,
      maxMemoryInMB=MAX_MEMORY_MB
    )
    model = rfc.fit(final_vectorized_features)

    for base_path in [REGISTRY_PATH, PRODUCTION_PATH]:
      model.write().overwrite().save(
        "{}/spark_random_forest_classifier.flight_delays.5.0.bin".format(base_path)
      )

    ml_bucketized_features.unpersist()

    # ── Evaluación ────────────────────────────────────────────────────────
    predictions = model.transform(final_vectorized_features)

    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    evaluator = MulticlassClassificationEvaluator(
      predictionCol="Prediction",
      labelCol="ArrDelayBucket",
      metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print("Accuracy = {}".format(accuracy))

    predictions.groupBy("Prediction").count().show()
    predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)

    # ── Loguear en MLflow ─────────────────────────────────────────────────
    # Métricas
    mlflow.log_metric("accuracy", accuracy)

    # Parámetros del modelo
    mlflow.log_param("maxBins",        MAX_BINS)
    mlflow.log_param("maxMemoryInMB",  MAX_MEMORY_MB)

    # Trazabilidad de datos y rutas
    mlflow.log_param("input_path",       INPUT_PATH)
    mlflow.log_param("input_rows",       input_rows)
    mlflow.log_param("run_output_path",  REGISTRY_PATH)
    mlflow.log_param("run_timestamp",    run_timestamp)
    mlflow.log_param("spark_version",    "4.1.1")
    mlflow.log_param("iceberg_version",  "1.10.0")

    # Tags
    mlflow.set_tag("stage",           "candidate")
    mlflow.set_tag("run_timestamp",   run_timestamp)

    # Modelo completo en MLflow para replicabilidad futura
    mlflow.spark.log_model(
      spark_model=model,
      artifact_path="random_forest_model",
      registered_model_name="flight_delay_classifier"
    )

    print("MLflow run_id: {} logged with accuracy: {}".format(
      run.info.run_id, accuracy
    ))
    print("Run {} saved to registry and promoted to production".format(run_timestamp))

if __name__ == "__main__":
  main()