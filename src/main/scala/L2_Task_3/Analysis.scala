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
    averageDF.show(100, truncate = false)

    import spark.implicits._

    // Add generation_indicator column based on LV ActivePower values
    val resultDF = averageDF.withColumn("generation_indicator",
      when($"LV ActivePower (kW)" < 200, "Low")
        .when($"LV ActivePower (kW)" >= 200 && $"LV ActivePower (kW)" < 600, "Medium")
        .when($"LV ActivePower (kW)" >= 600 && $"LV ActivePower (kW)" < 1000, "High")
        .when($"LV ActivePower (kW)" >= 1000, "Exceptional")
    )

    resultDF.show(truncate = false)
    // Create a DataFrame with the provided JSON
    val json =
      """
        |[
        |{
        |'sig_name': 'LV ActivePower (kW)',
        |'sig_mapping_name': 'active_power_average'
        |},{
        |'sig_name': 'Wind Speed (m/s)',
        |'sig_mapping_name': 'wind_speed_average'
        |}{
        |'sig_name': 'Theoretical_Power_Curve (KWh)',
        |'sig_mapping_name': 'theo_power_curve_average'
        |},{
        |'sig_name' : 'Wind Direction (°)',
        |'sig_mapping_name': 'wind_direction_average'
        |}
        |]
        |""".stripMargin

    val jsonDF = spark.read.json(Seq(json).toDS())
    jsonDF.show(false)
    val mappingArray = jsonDF.collect()
    val joinedDF = resultDF.join(broadcast(jsonDF), col("sig_name") === col("sig_mapping_name"), "left_outer")
    var finalDF = joinedDF
    for (row <- mappingArray) {
      val sigMappingName = row.getAs[String]("sig_mapping_name")
      val sigName = row.getAs[String]("sig_name")

      finalDF = finalDF.withColumnRenamed(sigName, sigMappingName)
    }

    finalDF.show(false)

    val resultDF22 = finalDF.drop("sig_name", "sig_mapping_name")

    resultDF22.show(false)

  }
}