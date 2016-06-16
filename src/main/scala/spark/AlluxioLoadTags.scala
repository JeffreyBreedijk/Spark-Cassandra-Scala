package spark

import org.apache.spark.{SparkConf, SparkContext}

object AlluxioLoadTags  {

  case class Consolidation(key: String, value: String, start: String, end: String, sum: Double, min: Double, max: Double, counter: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val tags = sc.textFile("/Users/JeffreyBreedijk/Downloads/spark-tags2")
      .map(line => line.split(",").toList.map(_.trim))
      .map(b => (b.head, b(1), b(2), b(3)))
      .saveAsTextFile("alluxio://localhost:19998/tags")


  }

}
