package com.roundyuan.core.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort5 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("sort1").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.parallelize(Array("zhangsan 99 28","lisi 87 28","wangwu 99 26","zhaoliu 88 23"))

    val users= lines.map(line=>{
      val properties: Array[String] = line.split(" ")
      val name: String = properties(0)
      val pv: Int = properties(1).toInt
      val age: Int = properties(2).toInt
      (name,pv,age)
    })
    val sorted = users.sortBy(tp=>(-tp._3,tp._2))
    val usersList = sorted.collect()
    usersList.foreach(println)
  }
}