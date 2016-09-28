name := "spark-training"

version := "1.0"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.3"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"