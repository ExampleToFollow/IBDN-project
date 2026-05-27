name := "spark-kafka-predictor"

version := "0.1"

// Versión de Scala obligatoria
scalaVersion := "2.13.0"

// Versión de Spark obligatoria
val sparkVersion = "4.1.1"

libraryDependencies ++= Seq(
  // Spark core y SQL
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  // Kafka Structured Streaming
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  // Cassandra connector
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1"
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
)

// Empaquetado como fat jar
assembly / assemblyJarName := "spark-kafka-predictor-assembly.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"             => MergeStrategy.concat
  case x                             => MergeStrategy.first
}

// Opciones de compilación
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)