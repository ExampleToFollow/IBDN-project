#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)

APP_NAME = "ingest_flights_to_iceberg"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

RAW_INPUT_PATH = "s3a://raw/simple_flight_delay_features.jsonl.bz2"
ICEBERG_TABLE = "lakehouse.flights"


def build_spark_session():
    return (
        SparkSession.builder
        .appName(APP_NAME)

        # Catálogo Iceberg
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg")

        # Extensiones Iceberg
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        # Conexión a MinIO usando S3A
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .getOrCreate()
    )


def get_flight_schema():
    return StructType([
        StructField("ArrDelay", DoubleType(), True),
        StructField("CRSArrTime", TimestampType(), True),
        StructField("CRSDepTime", TimestampType(), True),
        StructField("Carrier", StringType(), True),
        StructField("DayOfMonth", IntegerType(), True),
        StructField("DayOfWeek", IntegerType(), True),
        StructField("DayOfYear", IntegerType(), True),
        StructField("DepDelay", DoubleType(), True),
        StructField("Dest", StringType(), True),
        StructField("Distance", DoubleType(), True),
        StructField("FlightDate", DateType(), True),
        StructField("FlightNum", StringType(), True),
        StructField("Origin", StringType(), True),
    ])


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = get_flight_schema()

    print("Leyendo datos crudos desde MinIO...")
    print(f"Ruta origen: {RAW_INPUT_PATH}")

    flights_df = spark.read.json(
        RAW_INPUT_PATH,
        schema=schema
    )

    print("Schema leído:")
    flights_df.printSchema()

    print("Muestra de datos:")
    flights_df.show(5, truncate=False)

    total_rows = flights_df.count()
    print(f"Total de registros leídos: {total_rows}")

    print("Creando/escribiendo tabla Iceberg...")
    print(f"Tabla destino: {ICEBERG_TABLE}")

    (
        flights_df
        .writeTo(ICEBERG_TABLE)
        .using("iceberg")
        .createOrReplace()
    )

    print("Tabla Iceberg creada correctamente.")

    print("Verificando lectura desde Iceberg...")
    iceberg_df = spark.table(ICEBERG_TABLE)

    iceberg_df.printSchema()
    iceberg_df.show(5, truncate=False)

    print(f"Total de registros en Iceberg: {iceberg_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()