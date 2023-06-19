package L2_Task_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MorningConsume {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Trying to run")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val topic = "checkpoint-data"

    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("name", StringType)
      )
    )

    // Read the entire data from Kafka topic as a batch query
    val batchDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val batchJsonDF = batchDF.selectExpr("CAST(value AS STRING) AS json")

    // Convert the JSON data to structured DataFrame using the provided schema
    val batchData = batchJsonDF.select(from_json(batchJsonDF("json"), schema).as("data"))

    // Extract the fields from the structured DataFrame
    val batchExtractedDF = batchData.select("data.*")

    // Show the entire data before starting the streaming query
    batchExtractedDF.show(1000000,truncate = false)

    // Streaming query
    val streamingQuery = startStreamingQuery(spark, topic, schema)

    // Wait for the streaming query to finish
    streamingQuery.awaitTermination()
  }

  private def startStreamingQuery(spark: SparkSession, topic: String, schema: StructType): StreamingQuery = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest") // Start from the beginning
      .option("failOnDataLoss", "false")
      .load()

    val jsonDF = df.selectExpr("CAST(value AS STRING) AS json")

    // Convert the JSON data to structured DataFrame using the provided schema
    val jsonData = jsonDF.select(from_json(jsonDF("json"), schema).as("data"))

    // Extract the fields from the structured DataFrame
    val extractedDF = jsonData.select("data.*")

    // Show previous and appended data using the "update" output mode
    extractedDF.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .option("checkpointLocation", "C:\\tmp\\checkpoints") // Specify the checkpoint location
      .start()
  }
}
