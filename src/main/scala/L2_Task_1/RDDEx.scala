package L2_Task_1

import org.apache.spark.sql.SparkSession

object RDDEx {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder ()
      .master ( "local" )
      .getOrCreate ()

    spark.sparkContext.setLogLevel ( "OFF" )

    val data = Seq (
      ("ID", "NAME", "PHONE", "ADDRESS"),
      ("10", "Abhishek", "8580989207", "Himachal")
    )
    val columns = Seq ( "ID", "Name", "Phone", "Address" )
    val newDF = spark.sparkContext.parallelize ( data )
    import spark.implicits._
    val todf = newDF.toDF ( columns: _* )
    todf.show ( false )

    todf.write.mode ( "overwrite" ).csv ( "C:\\tmp\\1Ex-31may" )
    println ( "Without the InferSchema" )
    //    spark.read.csv("C:\\tmp\\output\\1Ex-31may").printSchema()
    spark.read.csv ( "C:\\tmp\\stream.csv" ).printSchema ()
    println ( "With InferSchema :" )
    spark.read.option ( "header", "true" ).option ( "Delimiter", "|" ).option ( "inferSchema", "true" ).csv ( "C:\\tmp\\stream.csv" ).printSchema ()
    spark.read.option ( "inferSchema", value = true ).csv ( "C:\\tmp\\testingCSV.csv" ).printSchema ()

    //Creating RDD from the text file

    val readRDD = spark.sparkContext.textFile ( "C:\\tmp\\data.txt" )
    readRDD.collect ().foreach ( println )
    //Split
    val splitRDD = readRDD.flatMap ( f => f.split ( " " ) )
    splitRDD.foreach ( f => println ( f ) )

    //Map
    val mapRDD = splitRDD.map ( f => (f, 1) )
    mapRDD.foreach ( f => println ( f ) )

    //Filter

    val filterRDD = mapRDD.filter ( f => f._1.startsWith ( "a" ) )
    filterRDD.foreach ( f => println ( f ) )
    //Reduce
    println ( "Reduce transformation" )
    val reduceRDD = mapRDD.reduceByKey ( _ + _ )
    reduceRDD.foreach ( f => println ( f ) )
    //Swapping words and count and sorting by key
    println ( "Swapping and reducing by the key" )
    val finalRDD = reduceRDD.map ( f => (f._2, f._1) ).sortByKey ()
    finalRDD.foreach ( f => println ( f ) )

    //Count
    val rddCount = finalRDD.count ()
    println ( "Word count is " + rddCount )

    //Max Action
    val maxrdd = finalRDD.max ()
    println ( "Max is" + maxrdd._1, maxrdd._2 )

    //RDD Shuffling
    val simpleData = Seq ( ("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Raman", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )
    val columns1 = Seq ( "employee_name", "department", "state", "salary", "age", "bonus" )

    val shuffleDF = simpleData.toDF ( columns1: _* )
    shuffleDF.show ( false )
    val groupby1 = shuffleDF.groupBy ( "state", "department", "salary" )
    groupby1.count ().show ( false )

    //Collect As List
    val coldata = shuffleDF.collectAsList ().stream()
    println(coldata)
    coldata.forEach(println)

//    shuffleDF.write
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("topic", "Spark Topic")
//      .save("C:\\tmp\\output\\spark-kafka")
    while (true) {
      var i = 600000
      Thread.sleep ( i )
     i += i+1
    }
  }

}
