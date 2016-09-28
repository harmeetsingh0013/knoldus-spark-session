package week3.homework

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CustomReceiverApp extends App {

  val sc = new SparkConf().setAppName("customSqlStream").setMaster("local[8]")

  val ssc = new StreamingContext(sc, Seconds(10))

  val customReceiver = ssc.receiverStream(new SparkStreamSQLReceiver("user"))
  customReceiver.foreachRDD(user => user.foreach(user => println(user)))

  ssc.start()
  ssc.awaitTermination()
}
