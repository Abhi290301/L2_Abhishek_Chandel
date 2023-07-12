package Testing

import org.apache.spark.sql.SparkSession

object SQLDataWriting {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaConsumerJob")
      .master("local")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    val pgURL = "jdbc:postgresql://localhost:5432/TurbineData"
    val pgProperties = new java.util.Properties()
    pgProperties.setProperty("user", "postgres")
    pgProperties.setProperty("password", "123456")
    val tableName = "FinalPerDayData"

    val df3 = spark.read
      .jdbc(pgURL, tableName, pgProperties)

    // Perform operations on the DataFrame
    df3.show()

    df3.write
      .format("csv")
      .option("header", "true")
      .mode("ignore")
      .save("c:\\tmp\\Task5\\Postgres\\PerDayData")

  }

}
