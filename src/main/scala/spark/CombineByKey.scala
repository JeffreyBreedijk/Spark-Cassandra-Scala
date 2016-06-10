package spark

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey extends App{

    case class Consolidation(id: String, sum: Double, min: Double, max: Double, count: Int)

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    sc.cassandraTable("tsap","processed_measurements")
      .select("measurement_source", "measurement_type", "measurement_timestamp", "measurement")
      .filter(row => row.getString("measurement_type") == "INTERVAL")
      .map(row => (row.getString("measurement_source") , row.getDouble("measurement")))
      .combineByKey(
        (measure:Double) => (measure, measure, measure, 1),
        (res:(Double, Double, Double, Int), measure:Double) => (
          res._1 + measure,
          if (measure < res._2) measure else res._2,
          if (measure > res._3) measure else res._3,
          res._4 + 1),
        (res1:(Double,Double,Double,Int), res2:(Double,Double,Double,Int)) => (
          res1._1 + res2._1,
          if (res1._2 < res2._2) res1._2 else res2._2,
          if (res1._3 > res2._3) res1._3 else res2._3,
          res1._4 + res2._4))
      .map(x => new Consolidation(x._1, x._2._1, x._2._2, x._2._3, x._2._4))
      .saveToCassandra("tsap", "cons")


}
