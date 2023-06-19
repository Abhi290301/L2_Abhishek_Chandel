package L2_Task_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeltaTableAnalysis")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._
    val deltaTablePath = "C:\\tmp\\output\\Task\\DeltaData"

    val deltaDF = spark.read.format("delta").load(deltaTablePath)

    // Print the Delta Table
    println("Delta table prints:")
    deltaDF.show(truncate = false)

    // Printing the data points per day
    println("Showing no. of data points per day")
    val datapointPerDayDF = deltaDF
      .groupBy("signal_date")
      .agg(countDistinct("signal_ts").alias("datapoints"))
      .sort("signal_date")
    datapointPerDayDF.show(truncate = false)

    // Calculate the average value of signals per hour
    println("Printing the average value of signals per hour:")
    deltaDF.printSchema() // Check the schema for column names

    val averageDF = deltaDF
      .withColumn("hour", hour(col("signal_ts")))
      .groupBy("hour")
      .agg(
        avg(col("signals.`LV ActivePower (kW)`")).as("LV ActivePower (kW)"),
        avg(col("signals.`Wind Speed (m/s)`")).as("Wind Speed (m/s)"),
        avg(col("signals.`Theoretical_Power_Curve (KWh)`")).as("Theoretical_Power_Curve (KWh)"),
        avg(col("signals.`Wind Direction (°)`")).as("Wind Direction (°)")
      )
      .filter(col("hour") =!= 0)
      .sort("hour")

    averageDF.show(truncate = false)


    val resultDF = averageDF.withColumn("generation_indicator",
      when($"LV ActivePower (kW)" < 200, "Low")
        .when($"LV ActivePower (kW)" >= 200 && $"LV ActivePower (kW)" < 600, "Medium")
        .when($"LV ActivePower (kW)" >= 600 && $"LV ActivePower (kW)" < 1000, "High")
        .when($"LV ActivePower (kW)" >= 1000, "Exceptional")
    )

    resultDF.show(truncate = false)

    // DataFrame with the provided JSON
    val json =
      """
        |[
        |    {
        |        "sig_name": "LV ActivePower (kW)",
        |        "sig_mapping_name": "avg_LV_ActivePower"
        |    },
        |    {
        |        "sig_name": "Wind Speed (m/s)",
        |        "sig_mapping_name": "avg_Wind_Speed"
        |    },
        |    {
        |        "sig_name": "Theoretical_Power_Curve (KWh)",
        |        "sig_mapping_name": "avg_Theoretical_Power_Curve"
        |    },
        |    {
        |        "sig_name": "Wind Direction (°)",
        |        "sig_mapping_name": "avg_Wind_Direction"
        |    }
        |]
        |""".stripMargin


    spark.stop()
  }
}







//    //Calculate the Average of signals per date
//    println("Calculate the Average of signals per date : ")
//    val averageDFDate = deltaDF
//      .withColumn("date", to_date(col("signal_ts")))
//      .groupBy("date")
//      .agg(
//        avg(col("signals.`LV ActivePower (kW)`")).as("LV ActivePower (kW)"),
//        avg(col("signals.`Wind Speed (m/s)`")).as("Wind Speed (m/s)"),
//        avg(col("signals.`Theoretical_Power_Curve (KWh)`")).as("Theoretical_Power_Curve (KWh)"),
//        avg(col("signals.`Wind Direction (°)`")).as("Wind Direction (°)")
//      )
//      .filter(col("date").isNotNull)
//      .sort("date")
//    averageDFDate.show(false)
//
//    val resultDFDate = averageDFDate.withColumn("generation_indicator",
//      when($"LV ActivePower (kW)" < 200, "Low")
//        .when($"LV ActivePower (kW)" >= 200 && $"LV ActivePower (kW)" < 600, "Medium")
//        .when($"LV ActivePower (kW)" >= 600 && $"LV ActivePower (kW)" < 1000, "High")
//        .when($"LV ActivePower (kW)" >= 1000, "Exceptional")
//    )
//  resultDFDate.show(false)