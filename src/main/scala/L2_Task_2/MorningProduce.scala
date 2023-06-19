package L2_Task_2

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object MorningProduce {
  def main(args: Array[String]): Unit = {
    val topic = "checkpoint-data"
    val bootstrapServers = "localhost:9092"

    // Kafka producer properties
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    try {
      val scanner = new java.util.Scanner(System.in, "UTF-8")

      while (true) {
        print("Enter data (id,name): ")
        val input = scanner.nextLine()

        val Array(id, name) = input.split(",").map(_.trim)

        val jsonRecord = s"""{"id": $id, "name": "$name"}"""

        val producerRecord = new ProducerRecord[String, String](topic, null, jsonRecord)
        producer.send(producerRecord)
      }
    } finally {
      producer.close()
    }
  }
}
