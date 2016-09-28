package week3.homework

import java.sql.{Connection, DriverManager}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class SparkStreamSQLReceiver(tableName: String) extends Receiver[User] (StorageLevel.MEMORY_AND_DISK_2) {

  var connection:Connection = null;

  override def onStart(): Unit ={
    println("On start in custom receiver ")
    new Thread("Mysql Receiver") {
      override def run() { receive }
    }.start()
  }

  override def onStop (): Unit = {
    println("On stop in custom receiver ")
  }

  private def receive: Unit = {
    val url = "jdbc:mysql://localhost/spark_stream_receiver"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "root"

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      val rs = statement.executeQuery("SELECT * FROM "+tableName)
      while (rs.next) {
        val user = User(rs.getInt("id"), rs.getString("uuid"), rs.getString("name"))
        store(user)
      }
    } catch {
      case e: Exception => e.printStackTrace
    }finally {
      connection.close
    }
    receive
  }
}
