package L2_Task_3

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SparkSession, functions}

object Subscriber {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaConsumerJob")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    spark.sparkContext.setCheckpointDir("C:\\tmp\\output\\Task\\CheckProd")

    // Read data from Kafka
    val kafkaBrokers = "localhost:9092"
    val kafkaTopic = "testing20"
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Process the streaming data
    val processedDF = kafkaDF.selectExpr("CAST(value AS STRING)")
    val processDF2 = kafkaDF.withColumn("value",col("value").cast(StringType))
    val splitColumns = functions.split(col("value"), ",")
    val columns = processDF2.select(
      splitColumns.getItem(0).as("Date/Time"),
      splitColumns.getItem(1).as("LV ActivePower (kW)"),
      splitColumns.getItem(2).as("Wind Speed (m/s)"),
      splitColumns.getItem(3).as("Theoretical_Power_Curve (KWh)"),
      splitColumns.getItem(4).as("Wind Direction (°)")
    )


    // Transform the data into the desired format
    val transformedDF = columns.select(
      to_date(col("Date/Time"), "dd MM yyyy").as("signal_date"),
      to_timestamp(col("Date/Time"), "dd MM yyyy HH:mm:ss").as("signal_ts"),
      functions.map(
        lit("LV ActivePower (kW)"), col("LV ActivePower (kW)"),
        lit("Wind Speed (m/s)"), col("Wind Speed (m/s)"),
        lit("Theoretical_Power_Curve (KWh)"), col("Theoretical_Power_Curve (KWh)"),
        lit("Wind Direction (°)"), col("Wind Direction (°)")
      ).as("signals"),
      current_date().as("create_date"),
      current_timestamp().as("create_ts")
    )
    // Write the transformed data in Delta format to a Delta directory
    val deltaDirectory = "C:\\tmp\\output\\Task\\DeltaData"
    val deltaSink = transformedDF.writeStream
      .format("delta")
      .option("checkpointLocation", "C:\\tmp\\output\\Task\\DeltaCheckpoint")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start(deltaDirectory)

    // Define the output sink
    val outputSink = columns.writeStream
      .format("console")
      .option("truncate","false")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    outputSink.awaitTermination()
    spark.stop()
  }
}
