package spark

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object CsvToMap extends App{


    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

    val tagcsv = sc.textFile("/Users/JeffreyBreedijk/Downloads/spark-tags")
   // val mcsv = sc.textFile("/Users/JeffreyBreedijk/Downloads/spark")

    val tags = tagcsv.map(line => line.split(",").toList.map(_.trim)).map(b => (b.head, (b(1), b(2)))).cache

   // val measurements = mcsv.map(line => line.split(",").toList.map(_.trim)).map(b => (b.head, (b(1), b(2), b(3), b(4), b(5)))).cache

    val mc = sc.cassandraTable("tsap","daily").where("date > ?", "2016-04-01 00:00")

    System.out.print(mc.count())



}
