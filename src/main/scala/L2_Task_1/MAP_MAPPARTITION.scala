package L2_Task_1

import org.apache.spark.sql._
import org.apache.spark.sql.types._
// Create util class
class Util extends Serializable {
  def combine(fname:String,mname:String,lname:String):String = {
    fname+","+mname+","+lname
  }
}

// Create object to run
object MapTransformation extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")
  private val structureData = Seq(
    Row("James","","Smith","36636","NewYork",3100),
    Row("Michael","Rose","","40288","California",4300),
    Row("Robert","","Williams","42114","Florida",1400),
    Row("Maria","Anne","Jones","39192","Florida",5500),
    Row("Jen","Mary","Brown","34561","NewYork",3000)
  )

  private val structureSchema = new StructType()
    .add("firstname",StringType)
    .add("middlename",StringType)
    .add("lastname",StringType)
    .add("id",StringType)
    .add("location",StringType)
    .add("salary",IntegerType)

  private val df2 = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData),structureSchema)
  df2.printSchema()
  df2.show(false)

  import spark.implicits._
  val util = new Util()
  private val df3 = df2.map( row=>{

    val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
    (fullName, row.getString(3),row.getInt(5))
  })
  private val df3Map =  df3.toDF("fullName","id","salary")

  df3Map.printSchema()
  df3Map.show(false)
  println("Before")
  df3Map.repartition(0).show(false)
  println("After")
  private val df4 = df2.mapPartitions( iterator => {
    val util = new Util()
    val res = iterator.map(f=>{
      val fullName = util.combine(f.getString(0),f.getString(1),f.getString(2))
      (fullName, f.getString(3),f.getInt(5))
    })
    res
  })
  private val df4part = df4.toDF("fullName","id","salary")
  df4part.printSchema()
  df4part.show(false)
}

