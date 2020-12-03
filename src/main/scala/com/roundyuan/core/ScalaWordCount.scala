package com.roundyuan.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ScalaWordCount")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val keyOne: RDD[(String, Int)] = words.map((_,1))
    val res: RDD[(String, Int)] = keyOne.reduceByKey(_+_)
    res.saveAsTextFile(args(1))
  }
}
