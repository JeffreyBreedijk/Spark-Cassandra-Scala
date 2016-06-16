package spark

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object LoadDayConsolidation {

  case class Consolidation(key:String, value: String, channel:String, start: String, end: String, sum: Double, min: Double, max: Double, counter: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)

    sc.textFile("alluxio://localhost:19998/consolidation/day/*")
      .map(t => t.replace("(", ""))
      .map(t => t.replace(")", ""))
      .map(line => line.split(",").toList.map(_.trim))
      .map(b => (b.head, b(1), b(2), b(3), b(4), b(5)))
      .filter(a => a._1 == "zipCode")
      .collect().foreach(println)


  }
}