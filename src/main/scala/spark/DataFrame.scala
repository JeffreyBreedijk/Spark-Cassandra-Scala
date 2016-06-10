package spark

//import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrame extends App{

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")

    val sc = new SparkContext(conf)

//    val csc = new CassandraSQLContext(sc)

//    csc.setKeyspace("tsap")
//
//    val data = csc.read.format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> "processed_measurements", "keyspace" -> "tsap"))
//      .load()


//
//    val data2 = csc.sql("SELECT measurement_source, measurement_timestamp, measurement FROM processed_measurements WHERE measurement_type = 'INTERVAL'")
}
