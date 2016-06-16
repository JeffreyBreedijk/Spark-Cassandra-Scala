package spark

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object AggregateMonthAllux {

  case class Consolidation(key:String, value: String, channel:String, start: String, end: String, sum: Double, min: Double, max: Double, counter: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)

    val start = sc.broadcast(org.joda.time.DateTime.parse("2016-04-01T00:00:00+02:00"))
    val end = sc.broadcast(org.joda.time.DateTime.parse("2016-05-01T00:00:00+02:00"))

    // measurements as  <(channelKey, channelType), (sum, min, max, count)>
    // apply combineByKey to create monthly aggregate per (channelKey, channelType)
    val measurements = sc.cassandraTable("tsap", "daily")
      .where("channel = ?", "GDT")
      .where("date > ?", start.value)
      .where("date <= ?", end.value)
      .map(r => ((r.getString("source"), r.getString("channel")), (r.getDouble("sum"), r.getDouble("min"), r.getDouble("max"), r.getInt("count"))))
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
      .cache()


    // tags as  <(channelKey, channelType), (tagKey, tagValue)>
    // ex:      <(12345, GDT), (zipCode, 3452)>
    val tagcsv = sc.textFile("alluxio://localhost:19998/tags")
      .map(t => t.replace("(", ""))
      .map(t => t.replace(")", ""))
      .map(line => line.split(",").toList.map(_.trim))
      .map(b => ((b.head, b(1)), (b(2), b(3))))
      .filter(a => a._2._1 != "asset" && a._2._2 != "channel")
      .cache()

    // join measurements and tags based on (channelKey, channelType)
    // map each row to <(tagKey, tagValue, channelType), (sum, min, max, count)>
    // apply combineByKey to create monthly aggregate per (tagKey, tagValue, channelType)
    val aggregated = measurements
      .join(tagcsv)
      .map(x => ((x._2._2._1, x._2._2._2, x._1._2), x._2._1))
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
      .map(a => (a._1._1, a._1._2, a._2._1, a._2._2, a._2._3, a._2._4))
      .saveAsTextFile("alluxio://localhost:19998/consolidation")


  }
}