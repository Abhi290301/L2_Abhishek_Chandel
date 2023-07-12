package TestSaturday

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object SourceProducer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Source Data")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val csvData = spark.read
      .option("header","true")
      .option("delimiter",",")
      .csv("C:\\tmp\\output\\Task\\File\\T1.csv")

    val kafkaBroker = "localhost:9092"
    val topicName = "saturday"

    val properties = new Properties()
    properties.put("bootstrap.servers","localhost:9092")
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    csvData.foreachPartition{
      partition: Iterator[org.apache.spark.sql.Row] =>
        val produce = new KafkaProducer[String,String](properties)
        partition.foreach{
          row =>
            val records = new ProducerRecord[String,String](topicName,row.mkString(","))

            produce.send(records)
        }
        produce.wait()
    }
    spark.stop()
  }
}
