package week3.workshop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by harmeet on 27/8/16.
  */

object StreamUnion extends App {

  val sc = new SparkConf().setAppName("joinStreams").setMaster("local[8]")

  val ssc = new StreamingContext(sc, Seconds(10))

  val stream1 = ssc.socketTextStream("localhost", 9999)
  val stream2 = ssc.socketTextStream("localhost", 9999)

  val words1 = stream1.flatMap(_.split(" "))
  val words2 = stream2.flatMap(_.split(" "))

  val wordCount1 = words1.map(a => (a, 1)).reduceByKey(_ + _)
  val wordCount2 = words2.map(a => (a, 1)).reduceByKey(_ + _)

  wordCount1.union(wordCount2).print()

  ssc.start()
  ssc.awaitTermination()
}
