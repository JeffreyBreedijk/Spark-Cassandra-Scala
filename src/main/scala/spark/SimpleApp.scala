package spark

import com.datastax.spark.connector._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object SimpleApp extends App{

    case class Consolidation(tagkey:String, tagvalue:String, start:Long, end:Long, sum: Double, min: Double, max: Double, count: Int)

    val start = org.joda.time.DateTime.parse("2016-04-01T00:00:00+02:00")
    val end = org.joda.time.DateTime.parse("2016-05-01T00:00:00+02:00")

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    val measurements = sc.cassandraTable("tsap","daily").where("date > ?", start).where("date <= ?", end)
      .map(r => (r.getString("source"), (r.getDouble("sum"), r.getDouble("min"), r.getDouble("max"), r.getInt("count")))).cache()

    val tagcsv = sc.textFile("/Users/JeffreyBreedijk/Downloads/spark-tags").map(line => line.split(",").toList.map(_.trim)).map(b => (b.head, (b(1), b(2))))
      .cache()

    val uniqueTags = tagcsv.map(t => ((t._2._1, t._2._2), 1)).reduceByKey(_ + _).cache()

    val provinces = uniqueTags.filter(t => t._1._1 == "province").cache()

    val xs = provinces.map(p => aggregate(filterTag(p._1._1, p._1._2)))

    xs.collect().foreach(a => println())



  private def filterTag(tagName:String, tagValue:String): RDD[(String, (String, String))] = {
    tagcsv.filter(t => t._2._1 == tagName && t._2._2 == tagValue)
  }

  private def aggregate(tags:RDD[(String, (String, String))]): RDD[((String, String), (Double, Double, Double, Int))] = {
    measurements
      .join(tags)
      .map(x => ((x._2._2._1, x._2._2._2), (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4)))
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
      //.map(x => new Consolidation(x._1._1, x._1._2, start.getMillis, end.getMillis, x._2._1, x._2._2, x._2._3, x._2._4))


  }








}
