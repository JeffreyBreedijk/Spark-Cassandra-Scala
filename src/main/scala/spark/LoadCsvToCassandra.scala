package spark

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime


object LoadCsvToCassandra extends App {

  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)

  val measurements = sc.textFile("/Users/JeffreyBreedijk/Downloads/spark")
    .map(line => line.split(",").toList.map(_.trim))
    .map(b => (b.head, (new DateTime(b(1).toLong), b(2).toDouble, b(3).toDouble, b(4).toDouble, b(5).toInt)))

}
