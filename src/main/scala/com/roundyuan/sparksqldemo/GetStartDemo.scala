package com.roundyuan.sparksqldemo

import org.apache.spark.sql.{DataFrame, SparkSession}
object GetStartDemo {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("spark sql basic example").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._
    // create dataFrame
    val df: DataFrame = sparkSession.read.json("src/main/resources/people.json")

    // Print the schema in a tree format
    df.printSchema()
    // Select only the "name" column
    df.select("name").show()
    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // Select people older than 21
    df.filter($"age" > 21).show()
    // Count people by age
    df.groupBy("age").count().show()
    //创建视图
    df.createOrReplaceTempView("people")
    val sqlDF = sparkSession.sql("SELECT * FROM people")
    sqlDF.show()
    // Register the DataFrame as a global temporary view ，spark程序结束停止使用，其他session也可以访问
    df.createGlobalTempView("people")
    // Global temporary view is tied to a system preserved database `global_temp`
    sparkSession.sql("SELECT * FROM global_temp.people").show()
    // Global temporary view is cross-session
    sparkSession.newSession().sql("SELECT * FROM global_temp.people").show()

    df.show()

  }
}
