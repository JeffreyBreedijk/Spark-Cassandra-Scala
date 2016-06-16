package spark

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime


object LoadMeasurementsToCassandra extends App {

  case class Daily(source: String, channel: String, date:String, sum: Double, max: Double, min: Double, count:Int)

  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)

  val measurements = sc.textFile("/Users/JeffreyBreedijk/Downloads/spark_actual2")
    .map(line => line.split(",").toList.map(_.trim))
    .map(b => new Daily(b.head, b(1), new DateTime(b(2).toLong).toString, b(3).toDouble, b(4).toDouble, b(5).toDouble, b(6).toInt))
    .saveToCassandra("tsap", "daily")



}
