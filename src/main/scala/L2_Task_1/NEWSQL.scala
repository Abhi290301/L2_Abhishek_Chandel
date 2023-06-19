package L2_Task_1

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

object NEWSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Testing")

    val sc = new SparkContext(conf)
    println("Spark Context Created Successfully: " + sc.master)
    sc.stop()

    try {

      //Creating Entry Point Spark Session
      val spark = SparkSession.builder()
        .appName("Testing")
        .master("local")
        .getOrCreate()
      //Setting Log Level
      spark.sparkContext.setLogLevel("OFF")

      //Creating a dataframe
      val data = Seq((1, "Abhishek"), (2, "Akash"))
      import spark.implicits._
      val rdd = spark.sparkContext.parallelize(data)
      val columns = Seq("ID", "User")
      val rddDF = rdd.toDF(columns: _*)
      val cdf = spark.createDataFrame(rdd).toDF(columns: _*)

      //Setting Persistence Level
      rddDF.persist(StorageLevel.MEMORY_AND_DISK)
      rddDF.printSchema()
      cdf.printSchema()
      cdf.show()

      //Creating a New Table with some complex schema
      val tableData = Seq(
        Row(120, "Abhishek Chandel", "abhishekchandel@xenonstack.com", "Azure", "DataOps", 100000, 20000, "M", List("Scala,Spark,Java"), "HP", "IN"),
        Row(121, "Shashank Sharma", "shashank@xenonstack.com", "AWS", "CloudOps", 150000, 15000, "M", List("Go,React,Python"), "HP", "IN"),
        Row(122, "Sahil Rana", "sahilrana@xenonstack.com", "Azure", "CloudOps", 100000, 18000, "M", List("Go,React,Python"), "PUN", "IN"),
        Row(123, "Sahil Kaushik", "sahil@xenonstack.com", "AWS", "CloudOps", 100000, 19000, "M", List("Go,React,Python"), "HR", "IN"),
        Row(124, "Nikita", "nikita@xenonstack.com", "Azure", "DataOps", 10000, 13000, "F", List("Go,C++,Python"), "HP", "IN"),
        Row(125, "Garima", "garima@xenonstack.com", "AWS", "CloudOps", 105000, 16000, "F", List("Scala,Spark,Java"), "HR", "IN"),
        Row(126, "Shivani", "shivani@xenonstack.com", "AWS", "DataOps", 100500, 12000, "F", List(""), "HP", "IN"),
        Row(127, "Anchal", "anchal@xenonstack.com", "Azure", "CloudOps", 20000, 10000, "F", List("Go,Java,Python"), "HP", "IN"),
        Row(128, "Satish", "Satish@xenonstack.com", "Azure", "CloudOps", 45200, 19000, "", List("Scala,Spark,Java"), "Uk", "IN"),
        Row(129, "Shubham", "shubham@xenonstack.com", "AWS", "DataOps", 65000, 16000, "M", List("Scala,Spark,Java"), "HP", "IN"),
        Row(130, "Ritik", "ritik@xenonstack.com", "AWS", "DataOps", 65000, 15000, "T", List("Go,React,Python"), "HR", "IN")
      )

      val fields = new StructType(
        Array(
          StructField("EID", IntegerType, nullable = false),
          StructField("E-Name", StringType, nullable = false),
          StructField("E-email", StringType, nullable = true),
          StructField("Department", StringType, nullable = false),
          StructField("Platform", StringType, nullable = false),
          StructField("Salary", IntegerType, nullable = false),
          StructField("Bonus", IntegerType, nullable = false),
          StructField("Gender", StringType, nullable = true),
          StructField("Languages", ArrayType(StringType), nullable = true),
          StructField("States", StringType, nullable = true),
          StructField("Country", StringType, nullable = true)
        )
      )
      val rdd1 = spark.sparkContext.parallelize(tableData)

      val newCDF = spark.createDataFrame(rdd1, fields)

      newCDF.printSchema()
      newCDF.show(false)
      newCDF.write.mode("overwrite").parquet("C:\\tmp\\myDataCSV")
      spark.read.parquet("C:\\tmp\\myDataCSV").printSchema()
      spark.read.option("inferSchema", "true").parquet("C:\\tmp\\myDataCSV").printSchema()

      //Using Distinct
      //Counting Data Entries before the distinct method
      println("Total no. of values before distinct: " + newCDF.count())
      val distinctValue = newCDF.distinct()
      distinctValue.show(false)

      //Count The data After Distinct
      println("Total no. of values After Distinct: " + distinctValue.count())

      //Where Clause
      distinctValue.where(distinctValue("Department") === "AWS").printSchema()
      distinctValue.where(distinctValue("Department") === "AWS").show(false)
      distinctValue.where(distinctValue("Gender") === "T" && distinctValue("E-Name") === "Ritik").show(false)

      //Estimating the size of the DataFrame
      val estimating = SizeEstimator.estimate(newCDF)
      println(s"The size of the frame is ${estimating / 1000000} mb")

      //Drop a column
      newCDF.drop("States").printSchema()

      //GroupBy
      newCDF.groupBy("Department").sum("salary").show(false)
      newCDF.groupBy("Department").min("salary").show(false)
      newCDF.groupBy("Department", "States").sum("salary").show(false)
      newCDF.groupBy("Department", "States")
        .agg(
          sum("Salary").as("Sum_Salary"),
          min("Salary").as("Min. Salary"),
          avg("Salary").as("Avg_Salary"),
          avg("Bonus").as("Avg_Bonus")
        ).where(col("Avg_Bonus") >= 3000).show(false)

      //Writing a parquet file
      try {
        val data = Seq(("James ", "A", "Smith", "36636", "M", 3000),
          ("Michael ", "Rose", "A", "40288", "M", 4000),
          ("Robert ", "A", "Williams", "42114", "M", 4000),
          ("Maria ", "Anne", "Jones", "39192", "F", 4000),
          ("Jen", "Mary", "Brown", "15546", "F", 1000))
        
        val columns = Seq("firstname", "middle name", "lastname", "dob", "gender", "salary")

        val df = data.toDF(columns: _*)
        df.write.mode(SaveMode.Ignore).parquet("C:\\output\\example.parquet")
        df.write.mode(SaveMode.Ignore).partitionBy("gender", "salary").parquet("C:\\tmp\\output\\parquet.parquet")
        df.createOrReplaceTempView("DataTable")

        val parquetRead = spark.read.option("inferSchema", "true").parquet("C:\\tmp\\output\\parquet.parquet")
        parquetRead.createOrReplaceTempView("ParquetTable")
        val query = spark.sql("Select * from ParquetTable Where salary>=4000 and gender = 'M'")
        query.show(false)

        val query2 = spark.sql("Select * from ParquetTable where gender= 'M'")
        query2.show(false)

      }
      catch {
        case e: Exception => println(e.printStackTrace())

      }

      //2nd Example of writing parquet file

      newCDF.write.mode(SaveMode.Ignore).partitionBy("Department").parquet("C:\\tmp\\output\\Data1.parquet")
      newCDF.createOrReplaceTempView("Table")
      val query1 = spark.sql("Select * from Table where Department = 'AWS'")
      query1.show(false)

      //With Columns
      val newCDF2 = newCDF.withColumn("CODE", lit("XS/IN"))
      newCDF2.show(false)

      //With Columns Renamed
      newCDF2.withColumnRenamed("CODE", "COMPANY CODE").withColumnRenamed("SALARY", "InHand Credit").show(false)

      //Reading JSON file
      val readJSON = spark.read.option("multiline", "true").json("C:\\tmp\\multiline-zipcode.json")
      readJSON.printSchema()
      readJSON.show(false)

      //Customize schema for the json
      val df_with_schema = spark.read.option("multiline", "true")
        .json("C:\\tmp\\datajson.json")
      df_with_schema.printSchema()
      df_with_schema.show(false)

      val read = spark.read.option("multiline", "true").option("inferSchema", "true").json("C:\\tmp\\simple_zipcodes.json")
      read.printSchema()
      read.show(false)

      //Reading CSV files
      val readCSV = spark.read.option("multiline", "true").option("header", "true").csv("C:\\tmp\\stream.csv")
      readCSV.printSchema()
      readCSV.show(false)
    }
    catch {
      case e: Exception => println(e)
    }
    while (true) {
      Thread.sleep(60000)

    }
  }
}