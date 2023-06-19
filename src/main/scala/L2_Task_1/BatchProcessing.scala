package L2_Task_1

import org.apache.spark.sql.SparkSession

object BatchProcessing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder ()
      .appName ( "Batch processing" )
      .master ( "local" )
      .getOrCreate ()

    val sc = spark.sparkContext
    sc.setLogLevel ( "OFF" )
    val rdd = spark.read.option ( "Delimeter", "|" ).option ( "inferSchema", "true" ).csv ( "C:\\tmp\\projects_smaller.csv" )
    val columns = Seq ( "ID", "Name", "EID", "SNAme", "YNAme", "hds", "jhsdh", "hdgk", "dgfks", "sfssd" )
    val df = rdd.toDF ( columns: _* )
    df.printSchema ()
    df.show ( 1000, truncate = false )
    rdd.printSchema ()
    val listRdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2))

    def param0 = (accu: Int, v: Int) => accu + v

    def param1 = (accu1: Int, accu2: Int) => accu1 + accu2

    println("output 1 : " + listRdd.aggregate(2)(param0, param1))

  }
}
