package L2_Task_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ConsumerWindowing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WindowExample")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val topic = "checkpoint-data"

    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )
    )

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val jsonDF = df.selectExpr("CAST(value AS STRING) AS json")

    val jsonData = jsonDF.select(from_json(col("json"), schema).as("data"))

    val extractedDF = jsonData.select("data.*")
      .withColumn("timestamp",current_timestamp())

    // Define the window duration and sliding interval
    val windowDuration = "4 seconds"
    val slidingInterval = "2 seconds"

    val windowedDF = extractedDF
      .groupBy(window(col("timestamp"), windowDuration, slidingInterval))//Use of window
      .agg(collect_list(col("id")).as("ids"), collect_list(col("name")).as("names"))
      .withWatermark("timestamp","5 seconds") // Date data handling
      .orderBy(col("window").getField("start").desc)


    val query = windowedDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
