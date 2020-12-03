package com.roundyuan.core

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * cache spark的cache技术
 */
object CacheDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("cacheDemo").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val logRDD: RDD[String] = sc.textFile("hdfs://192.168.88.21:9000/sparkLearn/gc.log-202006271541")
    //缓冲
    val cacheRDD: RDD[String] = logRDD.cache()
    //persist
    val logRDDPersist: RDD[String] = logRDD.persist(StorageLevel.MEMORY_AND_DISK)
    //懒加载
    cacheRDD.unpersist()
    val lines: Long = cacheRDD.count()
    println(lines)
  }
}
