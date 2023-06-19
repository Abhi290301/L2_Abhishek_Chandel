package L2_Task_1

import org.apache.spark.sql.SparkSession

// Define a case class for the data structure
case class Person(name: String, age: Int,marks:Int)

object DataSetExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RDD and Dataset Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    try {
      // Create an RDD from a sequence of data
      val rdd = spark.sparkContext.parallelize(Seq(
        Person("John", 30,300),
        Person("Alice", 25,300),
        Person("Bob", 35,200)
      ))

      // Convert RDD to Dataset using toDS() method
      import spark.implicits._
      val dataset = rdd.toDS
      dataset.show(false)
      val df1 = dataset.toDF()
      val rddcreate = df1.rdd
      rddcreate.foreach(println)

      // Perform operations on the Dataset
      dataset.createOrReplaceTempView("Table")
      val filteredDataset = dataset.filter(_.age > 30)
      val filteredDataset1 = dataset.where(dataset("age") > 30)
      filteredDataset1.show()
      val whereDataset = spark.sql("Select * from Table where age > 30")
      val count = filteredDataset.count()

      // Show the results
      filteredDataset.show()
      whereDataset.show()
      println(s"Count: $count")
    } finally {
      spark.stop()
    }
  }
}
