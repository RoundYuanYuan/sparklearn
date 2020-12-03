package com.roundyuan.core

import org.apache.spark.sql.SparkSession

object ReadJsonByDataFrame {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
    val session = sparkSession.appName("jsonTra").master("local[2]").getOrCreate()
    val dfs = session.read.json("F:\\5-File\\7-code\\SparkLearn\\file\\student.json")
    dfs.filter("age>12").show()
  }
}
