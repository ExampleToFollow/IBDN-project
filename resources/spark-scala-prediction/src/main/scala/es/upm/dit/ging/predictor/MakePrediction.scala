package es.upm.dit.ging.predictor

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{concat, from_json, lit}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.oss.driver.api.core.CqlSession
import java.net.InetSocketAddress

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    val sparkMaster           = sys.env.getOrElse("SPARK_MASTER",             "spark://spark-master:7077")
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS",  "kafka:9092")
    val requestTopic          = sys.env.getOrElse("REQUEST_TOPIC",            "flight-delay-ml-request")
    val responseTopic         = sys.env.getOrElse("RESPONSE_TOPIC",           "flight-delay-ml-response")
    val base_path             = sys.env.getOrElse("MODEL_BASE_PATH",          "s3a://models/production")

    val spark = SparkSession
      .builder()
      .appName("flight_prediction")
      .master(sparkMaster)
      .config("spark.sql.extensions",                      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local",                   "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type",              "hadoop")
      .config("spark.sql.catalog.local.warehouse",         "s3a://flights")
      .config("spark.hadoop.fs.s3a.endpoint",              "http://minio:9000")
      .config("spark.hadoop.fs.s3a.access.key",            "minioadmin")
      .config("spark.hadoop.fs.s3a.secret.key",            "minioadmin")
      .config("spark.hadoop.fs.s3a.path.style.access",     "true")
      .config("spark.hadoop.fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
      .config("spark.cassandra.connection.host",           "cassandra")
      .config("spark.cassandra.connection.port",           "9042")
      .getOrCreate()

    import spark.implicits._

    // Cargar modelos desde s3a://models/production/
    val arrivalBucketizerPath = "%s/arrival_bucketizer_2.0.bin".format(base_path)
    println("Loading bucketizer from: " + arrivalBucketizerPath)
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)

    val columns = Seq("Carrier", "Origin", "Dest", "Route")

    val stringIndexerModelPath = columns.map(n =>
      "%s/string_indexer_model_%s.bin".format(base_path, n)
    )
    val stringIndexerModel = stringIndexerModelPath.map(n => StringIndexerModel.load(n))

    val vectorAssemblerPath = "%s/numeric_vector_assembler.bin".format(base_path)
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    val randomForestModelPath = "%s/spark_random_forest_classifier.flight_delays.5.0.bin".format(base_path)
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    // Leer stream de Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", requestTopic)
      .load()
    df.printSchema()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin",        DataTypes.StringType)
      .add("FlightNum",     DataTypes.StringType)
      .add("DayOfWeek",     DataTypes.IntegerType)
      .add("DayOfYear",     DataTypes.IntegerType)
      .add("DayOfMonth",    DataTypes.IntegerType)
      .add("Dest",          DataTypes.StringType)
      .add("DepDelay",      DataTypes.DoubleType)
      .add("Prediction",    DataTypes.StringType)
      .add("Timestamp",     DataTypes.TimestampType)
      .add("FlightDate",    DataTypes.DateType)
      .add("Carrier",       DataTypes.StringType)
      .add("UUID",          DataTypes.StringType)
      .add("Distance",      DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index",  DataTypes.DoubleType)
      .add("Dest_index",    DataTypes.DoubleType)
      .add("Route_index",   DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))

    val flightFlattenedDf2 = flightNestedDf.selectExpr(
      "flight.Origin", "flight.DayOfWeek", "flight.DayOfYear", "flight.DayOfMonth",
      "flight.Dest", "flight.DepDelay", "flight.Timestamp", "flight.FlightDate",
      "flight.Carrier", "flight.UUID", "flight.Distance",
      "flight.Carrier_index", "flight.Origin_index", "flight.Dest_index", "flight.Route_index"
    )

    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route",
      concat(flightFlattenedDf2("Origin"), lit('-'), flightFlattenedDf2("Dest"))
    )

    val vectorizedFeatures = vectorAssembler
      .setHandleInvalid("keep")
      .transform(predictionRequestsWithRouteMod2)

    val finalVectorizedFeatures = vectorizedFeatures
      .drop("Carrier_index")
      .drop("Origin_index")
      .drop("Dest_index")
      .drop("Route_index")

    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    val finalPredictions = predictions
      .drop("indices")
      .drop("values")
      .drop("rawPrediction")
      .drop("probability")

    finalPredictions.printSchema()

    val kafkaOutputDf = finalPredictions
      .selectExpr("CAST(UUID AS STRING) AS key", "to_json(struct(*)) AS value")

    val kafkaQuery = kafkaOutputDf
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", responseTopic)
      .option("checkpointLocation", "/tmp/checkpoints/flight_prediction_kafka")
      .outputMode("append")
      .start()

    val cassandraQuery = finalPredictions
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoints/flight_prediction_cassandra")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val session = CqlSession.builder()
          .addContactPoint(new InetSocketAddress("cassandra", 9042))
          .withLocalDatacenter("datacenter1")
          .build()

        val stmt = session.prepare(
          """INSERT INTO agile_data_science.flight_delay_predictions
            |(uuid, origin, dest, carrier, flight_date, day_of_week,
            | day_of_month, day_of_year, dep_delay, distance, route, prediction)
            |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
        )

        batchDF.collect().foreach { row =>
          session.execute(stmt.bind(
            row.getAs[String]("UUID"),
            row.getAs[String]("Origin"),
            row.getAs[String]("Dest"),
            row.getAs[String]("Carrier"),
            row.getAs[java.sql.Date]("FlightDate").toLocalDate,
            row.getAs[Int]("DayOfWeek").asInstanceOf[java.lang.Integer],
            row.getAs[Int]("DayOfMonth").asInstanceOf[java.lang.Integer],
            row.getAs[Int]("DayOfYear").asInstanceOf[java.lang.Integer],
            row.getAs[Double]("DepDelay").asInstanceOf[java.lang.Double],
            row.getAs[Double]("Distance").asInstanceOf[java.lang.Double],
            row.getAs[String]("Route"),
            row.getAs[Double]("Prediction").toString
          ))
        }
        session.close()
      }
      .start()

    kafkaQuery.awaitTermination()
    cassandraQuery.awaitTermination()
  }
}