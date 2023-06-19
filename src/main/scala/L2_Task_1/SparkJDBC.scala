package L2_Task_1

import org.apache.spark.sql.SparkSession

import java.util.Properties

object SparkJDBC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JDBC Connection")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val getProp = new Properties()
    getProp.setProperty("driver","com.mysql.cj.jdbc.driver")
    getProp.put("user","root")
    getProp.put("password","123456")


    val jdbcread= spark.read
      .jdbc ( "jdbc:mysql://localhost:3306/emp", "employee", getProp )

    jdbcread.show(false)
  }

}
