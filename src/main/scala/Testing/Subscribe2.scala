package Testing

import org.apache.spark.sql.functions.{col, count, current_date, current_timestamp, lit, month, to_date, to_timestamp}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object Subscribe2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaConsumerJob")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    spark.sparkContext.setCheckpointDir("C:\\tmp\\output\\Task\\CheckProdSemis")

    // Read data from Kafka
    val kafkaBrokers = "localhost:9092"
    val kafkaTopic = "PG"
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Process the streaming data
    val processedDF = kafkaDF.selectExpr("CAST(value AS STRING)")
    val splitColumns = functions.split(col("value"), ",")
    val columns = processedDF.select(
      splitColumns.getItem(0).as("Date/Time"),
      splitColumns.getItem(1).as("LV ActivePower (kW)"),
      splitColumns.getItem(2).as("Wind Speed (m/s)"),
      splitColumns.getItem(3).as("Theoretical_Power_Curve (KWh)"),
      splitColumns.getItem(4).as("Wind Direction (°)")
    )

    val transformedDF1 = columns.select(
      to_date(col("Date/Time"), "dd MM yyyy").as("signal_date"),
      to_timestamp(col("Date/Time"), "dd MM yyyy HH:mm").as("signal_ts"),
      col("LV ActivePower (kW)").as("LV ActivePower (kW)"),
      col("Wind Speed (m/s)").as("Wind Speed (m/s)"),
      col("Theoretical_Power_Curve (KWh)").as("Theoretical_Power_Curve (KWh)"),
      col("Wind Direction (°)").as("Wind Direction (°)"),
      current_date().as("create_date"),
      current_timestamp().as("create_ts")
    )

    val transformedDF2 = columns.select(
      to_date(col("Date/Time"), "dd MM yyyy").as("signal_date"),
      col("LV ActivePower (kW)").as("LV ActivePower (kW)"),
      col("Wind Speed (m/s)").as("Wind Speed (m/s)"),
      col("Theoretical_Power_Curve (KWh)").as("Theoretical_Power_Curve (KWh)"),
      col("Wind Direction (°)").as("Wind Direction (°)")
    )
      .withColumn("month", month(col("signal_date")))
      .groupBy("month")
      .agg(
        count("*").as("count"),
        sum("LV ActivePower (kW)").as("total_LV_ActivePower"),
        sum("Wind Speed (m/s)").as("total_Wind_Speed"),
        sum("Theoretical_Power_Curve (KWh)").as("total_Theoretical_Power_Curve"),
        sum("Wind Direction (°)").as("total_Wind_Direction")
      )
      .drop("signal_date")
      .drop("LV ActivePower (kW)")
      .drop("Wind Speed (m/s)")
      .drop("Theoretical_Power_Curve (KWh)")
      .drop("Wind Direction (°)")
      .drop("create_date")
      .drop("create_ts")
    // Define the output sink
    val outputSink = transformedDF2.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
/*
    val pgURL = "jdbc:postgresql://localhost:5432/TurbineDataBase"
    val pgProperties = new java.util.Properties()
    pgProperties.setProperty("user", "postgres")
    pgProperties.setProperty("password", "123456")
    val postgreSink = transformedDF1.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
        batchDF.write
          .mode("overwrite")
          .jdbc(pgURL, "TurbineData", pgProperties)
      }
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Read data from PostgreSQL table
    val tableName = "TurbineData"
    val df: DataFrame = spark.read
      .jdbc(pgURL, tableName, pgProperties)

    // Perform operations on the DataFrame
    df.show()

    df.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("c:\\tmp\\output\\TurbineSQLDataSemis")

    postgreSink.awaitTermination()



 */
    outputSink.awaitTermination()
    spark.stop()
  }
}
