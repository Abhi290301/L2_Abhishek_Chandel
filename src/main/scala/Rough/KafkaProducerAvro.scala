package Rough

import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.avro.functions.to_avro

object KafkaProducerAvro {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AvroStreamingProducer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Define the Avro schema as a JSON string
    val avroSchemaStr = """
      {
        "type": "record",
        "name": "Person",
        "fields": [
          {"name": "id", "type": "int"},
          {"name": "name", "type": "string"},
          {"name": "age", "type": "int"},
          {"name": "address", "type": "string"}
        ]
      }
    """

    // Convert the Avro schema JSON string to Avro Schema
    val avroSchema = new Schema.Parser().parse(avroSchemaStr)

    // Convert the Avro schema to StructType
    val avroStructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[org.apache.spark.sql.types.StructType]

    // Read Avro files as a streaming source with the specified schema
    val avroData = spark.readStream
      .format("avro")
      .schema(avroStructType)
      .load("C:\\tmp\\Vishal\\Avrofile\\")

    // Convert DataFrame to Avro binary format
    val avroBinaryData = avroData.select(
      to_avro(avroData.col("id"), avroSchemaStr).alias("value")
    )


    // Publish Avro data to Kafka topic
    val query = avroBinaryData.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation","c:\\tmp\\Vishal\\Checkpointing")
      .option("topic", "vish1")
      .start()

    query.awaitTermination()
  }
}
