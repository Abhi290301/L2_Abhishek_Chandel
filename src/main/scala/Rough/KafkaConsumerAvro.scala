package Rough

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro

object KafkaConsumerAvro {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaAvroConsumer")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val kafkaBootstrapServers = "localhost:9092"
    val kafkaTopic = "vish1"
    val checkpointDir = "c:/tmp/checkpointVish"

    // Read Avro data from Kafka topic
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .load()

    // Convert Avro binary data to DataFrame
    val avroDF = kafkaDF
      .select(from_avro($"value", "avro_schema") as "data") // Provide the Avro schema here

    // Select desired columns from the Avro data
    val resultDF = avroDF.select("data.id", "data.name", "data.age", "data.address")

    // Start the streaming query to show the Avro data in DataFrame format
    val query = resultDF.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // Await the termination of the query
    query.awaitTermination()
  }
}
