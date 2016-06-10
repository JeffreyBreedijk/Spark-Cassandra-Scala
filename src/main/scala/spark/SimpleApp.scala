package spark

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object SimpleApp {

  case class Consolidation(ke: String, va: String, start: String, end: String, sum: Double, min: Double, max: Double, co: Int)

  def main (args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    val start = sc.broadcast(org.joda.time.DateTime.parse("2016-04-01T00:00:00+02:00"))
    val end = sc.broadcast(org.joda.time.DateTime.parse("2016-05-01T00:00:00+02:00"))

    val measurements = sc.cassandraTable("tsap", "daily")
      .where("date > ?", start.value).where("date <= ?", end.value)
      .map(r => (r.getString("source"), (r.getDouble("sum"), r.getDouble("min"), r.getDouble("max"), r.getInt("count"))))
      .cache()

    val t = sc.textFile("/Users/JeffreyBreedijk/Downloads/spark-tags").map(line => line.split(",").toList.map(_.trim))
      .map(b => (b.head, (b(1), b(2))))
      .filter(t => t._2._1 == "province" && t._2._2 == "Utrecht")
      .cache()

  }


  private def aggregate(measurements:RDD[(String, (Double, Double, Double, Int))],
                        tags:RDD[(String, (String, String))],
                        start:DateTime,
                        end:DateTime) = {
    measurements
      .join(tags).map(x => ((x._2._2._1, x._2._2._2), (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4)))
      .combineByKey(
        measure => measure,
        (daily1: (Double, Double, Double, Int), daily2: (Double, Double, Double, Int)) => (
          daily1._1 + daily2._1,
          if (daily1._2 < daily2._2) daily1._2 else daily2._2,
          if (daily1._3 > daily2._3) daily1._3 else daily2._3,
          daily1._4 + daily2._4),
        (daily1: (Double, Double, Double, Int), daily2: (Double, Double, Double, Int)) => (
          daily1._1 + daily2._1,
          if (daily1._2 < daily2._2) daily1._2 else daily2._2,
          if (daily1._3 > daily2._3) daily1._3 else daily2._3,
          daily1._4 + daily2._4))
      .map(x => new Consolidation(x._1._1, x._1._2, start.toString, end.toString, x._2._1, x._2._2, x._2._3, x._2._4))
      .saveToCassandra("tsap", "cons")

  }








}
