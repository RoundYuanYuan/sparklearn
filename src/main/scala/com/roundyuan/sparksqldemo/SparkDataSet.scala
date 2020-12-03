package com.roundyuan.sparksqldemo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkDataSet {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("spark dataset").getOrCreate()
    specifySchema(sparkSession)
  }

  /*
  spark sql 支持两种方法将现有的RDD转换为Datasets。
  1.使用反射推断RDD的schema 包含指定类型的对象，这种方式可以使代码更加简洁，当已经知道了schema时可以更好的工作。
  2.通过interface 实现比较麻烦，但是直到运行时才知道列及其类型的情况。
  方式一
   */
  def inferSchemaByReflect(sparkSession: SparkSession)={
    import sparkSession.implicits._
    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF=sparkSession.sparkContext.textFile("src/main/resources/people.txt").map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")
    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = sparkSession.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    val maps: Array[Map[String, Any]] = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    maps.foreach(map=>{
      val keys: Iterable[String] = map.keys
      keys.foreach(println)
      println("---------------")
      val values: Iterable[Any] = map.values
      values.foreach(println)
      println("==========")
    })

  }

  def specifySchema(spark: SparkSession)={
    //方式二
    val peopleRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/people.txt")
    val schemaString = "name age"
    val fields: Array[StructField] = schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,nullable = true))
    val schema: StructType = StructType(fields)
    val rowRDD: RDD[Row] = peopleRDD.map(_.split(",")).map(attr=>Row(attr(0),attr(1).trim))
    val peopledf: DataFrame = spark.createDataFrame(rowRDD,schema)
    peopledf.createOrReplaceTempView("people")
    val results: DataFrame = spark.sql("SELECT name FROM people")
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[String]
    import spark.implicits._
    results.map(attributes => "Name: " + attributes(0)).show()
  }

  /**
   * 创建dataSet
   * @param sparkSession
   */
  def createDataSet(sparkSession: SparkSession)={
    import sparkSession.implicits._
    // Encoders are created for case classes
    val dataset: Dataset[Person] = Seq(Person("Andy",32)).toDS()
    dataset.show()
    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect().foreach(println(_))
    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val datasets: Dataset[Person] = sparkSession.read.json("src/main/resources/people.json").as[Person]
    datasets.show()
  }
}
case class Person(name: String, age: Long)
