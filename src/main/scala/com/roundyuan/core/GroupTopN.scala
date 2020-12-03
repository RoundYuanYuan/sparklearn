package com.roundyuan.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
object GroupTopN {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val teacherLogRDD: RDD[String] = sc.textFile("hdfs://192.168.88.21:9000/sparkLearn/teacher.log")
    val subjectTeacherAndOne: RDD[((String, String), Int)] = teacherLogRDD.map(
      url => {
        val splits: Array[String] = url.split("/")
        val teacher: String = splits.last
        val subject: String = splits(2).split("[.]")(0)
        ((subject, teacher), 1)
      }
    )
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(_ + _)
//     添加分区器 1、问题分区数据太多 维持大小为 N 的可排序数组 2、多次suffer 再 reduceByKey ^ 阶段就分区
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(new SubjectPartitioner(subjects))
    val sorteds: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      //优化前
      //it.toList.sortBy(_._2).reverse.take(3).iterator
      //优化后 定义ArrayBuffer 大小为N
      var sortArray = new ArrayBuffer[((String, String), Int)]()
      while (it.hasNext) {
        val sigRecord: ((String, String), Int) = it.next()
        val len: Int = sortArray.length
        if (len < 4) {
          sortArray += ((sigRecord._1, sigRecord._2))
        } else {
          sortArray = sortArray.sortBy(_._2)
          sortArray(0) = ((sigRecord._1, sigRecord._2))
        }
      }
      sortArray.toIterator
    })
    val res1: Array[((String, String), Int)] = sorteds.collect()

    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
    //toList 将所有数据加载到内存可能会试崩掉 解决方式可以使用 RDD的过滤再排序
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))
    val res: Array[(String, List[((String, String), Int)])] = sorted.collect()

    for (elem <- res) {
      println(elem)
    }
    sc.stop()
  }
}
class SubjectPartitioner(subject:Array[String]) extends Partitioner{
  //分区的数量
  override def numPartitions: Int = subject.size
  var object2key = new mutable.HashMap[String,Int]()
  var i=0;
  for (elem <- subject) {
    object2key.put(elem,i)
    i+=1
  }
  //更具Key返回分区编号
  override def getPartition(key: Any): Int ={
    object2key(key.asInstanceOf[(String,String)]._1)
  }
}
