package Testing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object SubscribeComplete {
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

    //Initially Transformed Data
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
    val outputSink1 = transformedDF1.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    val pgURL = "jdbc:postgresql://localhost:5432/TurbineData"
    val pgProperties = new java.util.Properties()
    pgProperties.setProperty("user", "postgres")
    pgProperties.setProperty("password", "123456")
    val tableName = "TurbineDataComplete"
    val postgreSink1 = transformedDF1.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
        batchDF.write
          .mode("overwrite")
          .jdbc(pgURL, tableName, pgProperties)
      }
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()


    // Read data from PostgreSQL table

    val df: DataFrame = spark.read
      .jdbc(pgURL, tableName, pgProperties)

    // Perform operations on the DataFrame
    df.show()

    df.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("c:\\tmp\\output\\TurbineSQLDataComplete")




    //Print data as per day data
    val transformedDF2 = columns.select(
      to_date(col("Date/Time"), "dd MM yyyy").as("signal_date"),
      col("LV ActivePower (kW)").as("LV ActivePower (kW)"),
      col("Wind Speed (m/s)").as("Wind Speed (m/s)"),
      col("Theoretical_Power_Curve (KWh)").as("Theoretical_Power_Curve (KWh)"),
      col("Wind Direction (°)").as("Wind Direction (°)")
    )
      .withColumn("date", to_date(col("signal_date")))
      .groupBy("date")
      .agg(count("*").as("count"),
        sum("LV ActivePower (kW)").as("total_LV_ActivePower"),
        sum("Wind Speed (m/s)").as("total_Wind_Speed"),
        sum("Theoretical_Power_Curve (KWh)").as("total_Theoretical_Power_Curve"),
        sum("Wind Direction (°)").as("total_Wind_Direction")
      ).orderBy("date")
      .drop("signal_date", "LV ActivePower (kW)", "Wind Speed (m/s)", "Theoretical_Power_Curve (KWh)", "Wind Direction (°)", "create_date", "create_ts")
    val outputSinkDate = transformedDF2.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    val postgreSink2 = transformedDF2.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
        batchDF.write
          .mode("overwrite")
          .jdbc(pgURL, "TurbineData", pgProperties)
      }
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
/*
    // Read data from PostgreSQL table
    val tableNameDate = "TurbineDataDates"
    val df2 = spark.read
      .jdbc(pgURL, tableNameDate, pgProperties)

    // Perform operations on the DataFrame
    df2.show()

    df2.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("c:\\tmp\\output\\TurbineSQLDataPerDate")



 */


    //Data According to the months
    val transformedDFMonths = columns.select(
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
      .orderBy("month")
      .drop("signal_date", "LV ActivePower (kW)", "Wind Speed (m/s)", "Theoretical_Power_Curve (KWh)", "Wind Direction (°)", "create_date", "create_ts")

    // Define the output sink
    val outputSink3 = transformedDFMonths.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    val postgreSink3 = transformedDFMonths.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
        batchDF.write
          .mode("overwrite")
          .jdbc(pgURL, "TurbineData", pgProperties)
      }
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    /*
    // Read data from PostgreSQL table
    val tableNameMonths = "TurbineDataMonths"
    val df3 = spark.read
      .jdbc(pgURL, tableNameMonths, pgProperties)

    // Perform operations on the DataFrame
    df3.show()

    df3.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("c:\\tmp\\output\\TurbineSQLDataMonths")




     */

    //Quarterly Data
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
        sum("Wind Speed (m/s)").as("total_Wind_Speed"),
        sum("Theoretical_Power_Curve (KWh)").as("total_Theoretical_Power_Curve"),
        sum("Wind Direction (°)").as("total_Wind_Direction")
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
    val postgreSink4 = transformedDFQuarterly.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, _: Long) =>
        batchDF.write
          .mode("overwrite")
          .jdbc(pgURL, "TurbineData", pgProperties)
      }
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
/*
    // Read data from PostgreSQL table
    val tableNameQuarterly = "TurbineDataQuarterly"
    val df4 = spark.read
      .jdbc(pgURL, tableNameQuarterly, pgProperties)

    // Perform operations on the DataFrame
    df4.show()

    df4.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("c:\\tmp\\output\\TurbineSQLDataQuarterly")



 */


    outputSink1.awaitTermination()

    outputSinkDate.awaitTermination()

    outputSink3.awaitTermination()

    outputSink4.awaitTermination()

    postgreSink1.awaitTermination()
    postgreSink2.awaitTermination()
    postgreSink3.awaitTermination()
    postgreSink4.awaitTermination()
    spark.stop()
  }
}
