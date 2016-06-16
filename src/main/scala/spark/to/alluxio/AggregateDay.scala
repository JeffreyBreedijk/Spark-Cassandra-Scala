package spark.to.alluxio

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer


object AggregateDay {

   def main(args: Array[String]) {

     val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
     val sc = new SparkContext(conf)

     var localStart = org.joda.time.DateTime.parse("2016-04-01T00:00:00+02:00")
     val localEnd = org.joda.time.DateTime.parse("2016-05-01T00:00:00+02:00")

     val start = sc.broadcast(localStart)
     val end = sc.broadcast(localEnd)

     val dates = new ListBuffer[DateTime]
     while (localStart.isBefore(localEnd)) {
       localStart = localStart.plusDays(1)
       dates += localStart
     }

     // measurements as  <(channelKey, channelType), (date, sum, min, max, count)>
     val measurements = sc.cassandraTable("tsap", "daily")
       .where("channel = ?", "GDT")
       .where("date > ?", start.value)
       .where("date <= ?", end.value)
       .map(r => ((r.getString("source"), r.getString("channel")), (r.get[org.joda.time.DateTime]("date"), r.getDouble("sum"), r.getDouble("min"), r.getDouble("max"), r.getInt("count"))))
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

     for (dt <- dates.toList) {
       val y = sc.broadcast(dt)
       val measurement = measurements
         .filter(a => a._2._1.isEqual(y.value)) // Get all day measurements for a specific day
         .map(b => (b._1, (b._2._2, b._2._3, b._2._4, b._2._5))) // Map to <(channelKey, channelType), (sum, min, max, count)>, field date became irrelevant
       doAggregate(measurement, tagcsv, y.value.minusDays(1), y.value)
     }


     def doAggregate(measurements:RDD[((String, String), (Double, Double, Double, Int))],
                      tags:RDD[((String, String), (String, String))], start:DateTime, end:DateTime): Unit = {
       measurements
         .join(tags)
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
         .saveAsTextFile("alluxio://localhost:19998/consolidation/day/" + end)
     }



   }


 }