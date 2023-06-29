package L2_Task_5

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

import java.util.Properties

object Producer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StreamingToKafkaJob")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    // Configure Kafka producer
    val kafkaBrokers = "localhost:9092,localhost:9093,localhost:9094,localhost:9095"
    val kafkaTopic = "a190"
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBrokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Load data from CSV file
    val csvFilePath = "C:\\tmp\\output\\Task\\File\\T1.csv"
    val csvData = spark.read
      .option("header", value = true)
      .option("delimiter","\t")
      .csv(csvFilePath)

    // Publish records to Kafka from CSV data
    publishToKafka(csvData, kafkaTopic, props)


    // Read data from a streaming source
    val sourceTopic = "source"
    val kafkaParams = Map(
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "my_consumer_group_id"
    )

    val streamData = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", sourceTopic)
      .option("startingOffsets", "latest")
      .options(kafkaParams)
      .load()
      .selectExpr("CAST(value AS STRING)")

    // Publish records to Kafka from streaming data
    streamData.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        publishToKafka(batchDF, kafkaTopic, props)
      }
      .start()
      .awaitTermination()

    spark.stop()
  }

  def publishToKafka(data: DataFrame, topic: String, props: Properties): Unit = {
    data.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
      val producer = new KafkaProducer[String, String](props)
      partition.foreach { row =>
        val record = new ProducerRecord[String, String](topic, row.mkString(","))
        producer.send(record)
      }
      producer.close()
    }
  }
}
