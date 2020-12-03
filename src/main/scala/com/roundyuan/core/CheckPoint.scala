package com.roundyuan.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckPoint {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("cacheDemo").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("hdfs://192.168.88.21:9000/sparkLearn/checkdir")
    val logRDD: RDD[String] = sc.textFile("hdfs://192.168.88.21:9000/sparkLearn/teacher.log")
    val filterd: RDD[String] = logRDD.filter(_.contains("bigdata"))
    filterd.checkpoint()
    filterd.count()
  }
}
