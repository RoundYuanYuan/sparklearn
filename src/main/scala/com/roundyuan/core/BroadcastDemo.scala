package com.roundyuan.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("BroadcastDemo").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val lines: RDD[String] = sc.textFile("hdfs://192.168.88.21:9000/sparkLearn/word.txt")
    val strings="bigdata"
//    val blines: Broadcast[Array[String]] = sc.broadcast(strings)
//    val res: RDD[Boolean] = lines.map(_.contains(blines.value))
//    res.collect()
  }
}
