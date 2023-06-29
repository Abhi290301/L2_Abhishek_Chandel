package L2_Task_3

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Producer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSVToKafkaJob")
      .master("local")
      .getOrCreate()
spark.sparkContext.setLogLevel("OFF")
    // Read the CSV file
    val df = spark.read
      .option("header", value = true)
      .option("delimiter","\t")
      .csv("C:\\tmp\\output\\Task\\File\\T1.csv")

    // Configure Kafka producer
    val kafkaBrokers = "localhost:9092,localhost:9093,localhost:9094,localhost:9095"
    val kafkaTopic = "a"
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBrokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Publish records to Kafka
    df.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
      val producer = new KafkaProducer[String, String](props)
      partition.foreach { row =>
        val record = new ProducerRecord[String, String](kafkaTopic, row.mkString(","))
        producer.send(record)
      }
      producer.close()
    }

    spark.stop()
  }
}
