package com.cluster.predictor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkKafkaPredictor")
      .getOrCreate()
    import spark.implicits._

    println("Spark Kafka Predictor starting...")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "flight-delay-ml-request")
      .option("startingOffsets", "earliest")
      .load()

    df.printSchema()
    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")
    
    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)


    // ── 2. Parsear JSON recibido ────────────────────────────────
    val parsed = kafkaSource
      .selectExpr("CAST(value AS STRING) AS payload")
      .withColumn("uuid",        get_json_object($"payload", "$.uuid"))
      .withColumn("origin",      get_json_object($"payload", "$.origin"))
      .withColumn("dest",        get_json_object($"payload", "$.dest"))
      .withColumn("carrier",     get_json_object($"payload", "$.carrier"))
      .withColumn("flight_num",  get_json_object($"payload", "$.flight_num"))
      .withColumn("flight_date", get_json_object($"payload", "$.flight_date"))
      .withColumn("dep_delay",   get_json_object($"payload", "$.dep_delay"))
      .withColumn("distance",    get_json_object($"payload", "$.distance"))
      .withColumn("timestamp",   get_json_object($"payload", "$.timestamp"))
      .filter($"uuid".isNotNull && $"uuid" =!= "")

    // ── 3. Hardcodear predicción ────────────────────────────────
    val response = parsed
      .withColumn("prediction", lit("PREDICTION_MOCK_OK"))

    // ── 4. Preparar mensaje para Kafka ──────────────────────────
    val kafkaOutput = response
      .withColumn(
        "value",
        to_json(struct(
          $"uuid",
          $"origin",
          $"dest",
          $"carrier",
          $"flight_num",
          $"flight_date",
          $"dep_delay",
          $"distance",
          $"prediction",
          $"timestamp"
        ))
      )
      .select(
        $"uuid".cast("string").alias("key"),
        $"value"
      )

    // ── 5. Escribir en Kafka response ───────────────────────────
    val query = kafkaOutput.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "flight-delay-ml-response")
      .option("checkpointLocation", "/tmp/spark-checkpoint/kafka-response")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}