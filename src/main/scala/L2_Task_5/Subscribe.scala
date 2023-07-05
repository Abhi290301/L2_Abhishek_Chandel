package L2_Task_5

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{SparkSession, functions}

object Subscribe {
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
    val kafkaTopic = "semis1"
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

    // Define the output sink
    val outputSink = transformedDF1.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()


    val transformedDF2 = transformedDF1
      .withColumn("month", functions.month(col("signal_date")))
      .groupBy("month")
      .agg(
        functions.count("*").as("count"),
        functions.sum("LV ActivePower (kW)").as("total_LV_ActivePower"),
        functions.sum("Wind Speed (m/s)").as("total_Wind_Speed"),
        functions.sum("Theoretical_Power_Curve (KWh)").as("total_Theoretical_Power_Curve"),
        functions.sum("Wind Direction (°)").as("total_Wind_Direction")
      )


    val pgURL1 = "jdbc:postgresql://localhost:5432/postgres"
    val pgProperties = new java.util.Properties()
    pgProperties.setProperty("user", "postgres")
    pgProperties.setProperty("password", "123456")
    val postgreSink1 = transformedDF1.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
        batchDF.write
          .mode("overwrite")
          .jdbc(pgURL1, "SemisDataDate", pgProperties)
      }
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()


    val pgURL2 = "jdbc:postgresql://localhost:5432/TurbineData"
    val pgPropertiesMonths = new java.util.Properties()
    pgPropertiesMonths.setProperty("user", "superset")
    pgPropertiesMonths.setProperty("password", "superset")
    val postgreSink2 = transformedDF2.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
        batchDF.write
          .mode("overwrite")
          .jdbc(pgURL2, "SemisDataMonths", pgPropertiesMonths)
      }
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Read data from PostgreSQL table as Dates
    val tableName = "SemisDataDate"
    val dfDates = spark.read
      .jdbc(pgURL1, tableName, pgProperties)

    // Perform operations on the DataFrame
    dfDates.show()

    dfDates.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("c:\\tmp\\output\\TurbineSQLDataSemis")

    // Read data from PostgreSQL table
    val tableName2 = "SemisDataMoths"
    val dfMonths = spark.read
      .jdbc(pgURL1, tableName2, pgProperties)

    // Perform operations on the DataFrame as Months
    dfMonths.show()

    dfMonths.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("c:\\tmp\\output\\TurbineSQLDataSemis")

    postgreSink2.awaitTermination()
    // postgreSink1.awaitTermination()
    outputSink.awaitTermination()
    spark.stop()
  }
}
