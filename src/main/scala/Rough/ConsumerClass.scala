package Rough

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object ConsumerClass {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Testing")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val topic = "testing20"
    val server = "localhost:9092"
    val schema = StructType(Seq(
      StructField("DateTime", StringType),
      StructField("LV_ActivePower", DoubleType),
      StructField("Wind_Speed", DoubleType),
      StructField("Theoretical_Power_Curve", DoubleType),
      StructField("Wind_Direction", DoubleType)
    ))

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", server)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    import spark.implicits._

    val dataStream = kafkaStream
      .selectExpr("CAST(value AS STRING)")
      .select(split($"value", ",").as("split_values"))
      .select(
        $"split_values".getItem(0).as("DateTime"),
        $"split_values".getItem(1).cast(DoubleType).as("LV_ActivePower"),
        $"split_values".getItem(2).cast(DoubleType).as("Wind_Speed"),
        $"split_values".getItem(3).cast(DoubleType).as("Theoretical_Power_Curve"),
        $"split_values".getItem(4).cast(DoubleType).as("Wind_Direction")
      )

    val consoleQuery = dataStream.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    consoleQuery.awaitTermination()
  }
}
