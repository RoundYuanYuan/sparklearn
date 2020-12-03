package com.roundyuan.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopN {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val teacherLogRDD: RDD[String] = sc.textFile("hdfs://192.168.88.21:9000/sparkLearn/teacher.log")
    //预览数据
//    val sampleRDD: RDD[String] = teacherLogRDD.sample(false,0.2,2)
//    val strings: Array[String] = sampleRDD.collect()
//    strings.foreach(println)
    val teachers: RDD[String] = teacherLogRDD.map(x => {
      x.split("/").last
    })
    val teracherAndOne: RDD[(String, Int)] = teachers.map((_,1))
    val reduceRDD: RDD[(String, Int)] = teracherAndOne.reduceByKey(_+_)
    // 方法一、
//    val sorted: RDD[(String, Int)] = reduceRDD.sortBy(_._2,false)
//    val res3: Array[(String, Int)] = sorted.take(3)
    //方法二
    val res3=reduceRDD.top(3)(Ordering.by(e=>e._2))
    res3.foreach(a=>println(a._1+"|"+a._2))
    sc.stop()
  }
}
