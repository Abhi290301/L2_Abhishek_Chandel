package Testing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{SparkSession, functions}

object TurbineDataQuarterly {
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
    val kafkaTopic = "Friday1"
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
    val transformedDFQuarterly = columns.select(
      to_date(col("Date/Time"), "dd MM yyyy").as("signal_date"),
      col("LV ActivePower (kW)").as("LV ActivePower (kW)"),
      col("Wind Speed (m/s)").as("Wind Speed (m/s)"),
      col("Theoretical_Power_Curve (KWh)").as("Theoretical_Power_Curve (KWh)"),
      col("Wind Direction (°)").as("Wind Direction (°)")
    )
      .withColumn("quarter", quarter(col("signal_date")))
      .groupBy("quarter")
      .agg(
        count("*").as("count"),
        sum("LV ActivePower (kW)").as("total_LV_ActivePower"),
        avg("Wind Speed (m/s)").as("total_Wind_Speed"),
        sum("Theoretical_Power_Curve (KWh)").as("total_Theoretical_Power_Curve"),
        avg("Wind Direction (°)").as("total_Wind_Direction")
      )
      .orderBy("quarter")
      .drop("signal_date", "LV ActivePower (kW)", "Wind Speed (m/s)", "Theoretical_Power_Curve (KWh)", "Wind Direction (°)", "create_date", "create_ts")

    println("Quarterly Data Printing")
    val outputSink4 = transformedDFQuarterly.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    val pgURL = "jdbc:postgresql://localhost:5432/TurbineData"
    val pgProperties = new java.util.Properties()
    pgProperties.setProperty("user", "postgres")
    pgProperties.setProperty("password", "123456")
    val tableName = "FinalPerQuarterData"
    val postgreSink4 = transformedDFQuarterly.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
        batchDF.write
          .mode("overwrite")
          .jdbc(pgURL, tableName, pgProperties)
      }
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()


    // Read data from PostgreSQL table
    /*
        val df4 = spark.read
          .jdbc(pgURL, tableName, pgProperties)

        // Perform operations on the DataFrame
        df4.show()

        df4.write
          .format("csv")
          .option("header", "true")
          .mode("ignore")
          .save("c:\\tmp\\output\\TurbineSQLDataQuarterly")
     */
    outputSink4.awaitTermination()
  }
}
