name := "flight_prediction"

version := "0.1"

scalaVersion := "2.13.17"

val sparkVersion = "4.1.1"

Compile / mainClass := Some("es.upm.dit.ging.predictor.MakePrediction")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.1",
"com.datastax.oss" % "java-driver-core" % "4.17.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.4.1",
  "org.apache.iceberg" %% "iceberg-spark-runtime-4.0" % "1.10.0"
)
