package L2_Task_1

import org.apache.spark.sql.{SaveMode, SparkSession}

object Repartitioning_as_Key {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("Repartition Data by Keys")
      .master("local[*]")
      .getOrCreate()
    //    spark.sparkContext.setLogLevel("OFF")
    try {
      val data = Seq(
        ("John", 25, "USA"),
        ("Alice", 30, "Canada"),
        ("Bob", 28, "UK"),
        ("Jane", 35, "USA")
      )
      val rdd = spark.sparkContext.parallelize(data)
      import spark.implicits._
      val df = rdd.toDF("Name", "Age", "Country")
      df.printSchema()

      val repartitionedDf = df.repartition(2, $"Country") // Repartition the DataFrame by the "Country" column
      repartitionedDf.printSchema()
      repartitionedDf.where(repartitionedDf("Country") === "USA").show()
      repartitionedDf.filter(repartitionedDf("Country") === "USA").show()
      repartitionedDf.where($"Age" > 25).show()
      repartitionedDf.where("Age == '25'").show()
      repartitionedDf.filter($"Age" > 25).show()
      val outputPath = "C:\\tmp\\RepartitionBYKey"
      repartitionedDf.write
        .mode(SaveMode.Overwrite)
        .parquet(outputPath)
    }

    //By Coalesce
    try {
      val data = Seq(
        ("John", 25, "USA"),
        ("Alice", 30, "Canada"),
        ("Bob", 28, "UK"),
        ("Jane", 35, "USA")
      )

      import spark.implicits._
      val df = data.toDF("Name", "Age", "Country")

      val coalescedDf = df.coalesce(numPartitions = 2) // Coalesce the DataFrame into 2 partitions

      val outputPath = "C:\\tmp\\coalesce"
      coalescedDf.write
        .mode(SaveMode.Overwrite)
        .parquet(outputPath)
    }
    finally {
      while (true) {
        Thread.sleep(600000)
      }
    }

  }
}