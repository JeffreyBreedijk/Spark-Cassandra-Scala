import AssemblyKeys._

name := "Cassandra Spark Scala"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.4.0" % "provided",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.4.0"
)

assemblySettings
jarName in assembly := "cassandra-spark-scala.jar"
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)