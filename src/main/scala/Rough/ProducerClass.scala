package Rough

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object ProducerClass {
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

    val df = spark.readStream
      .format("csv")
      .option("header","true")
      .option("delimiter",",")
      .schema(schema)
      .load("C:\\tmp\\output\\Task\\File\\")

     import spark.implicits._
    val kafkaStream = df.select(
      to_json(
        struct($"DateTime", $"LV_ActivePower", $"Wind_Speed", $"Theoretical_Power_Curve", $"Wind_Direction")
      ).as("value")
    )

      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", server)
      .option("topic", topic)
      .option("checkpointLocation", "C:/tmp/CheckPoint")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

   df.writeStream
      .format("console")
     .outputMode(OutputMode.Append())
     .start()
     .awaitTermination()


      kafkaStream.awaitTermination()
  }

}


