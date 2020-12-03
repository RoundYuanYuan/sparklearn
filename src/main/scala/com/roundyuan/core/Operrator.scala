package com.roundyuan.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Operrator {
  def main(args: Array[String]): Unit = {
    //准北阶段
    val sparkConf: SparkConf = new SparkConf().setAppName("Operrator Demo").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //sum
    var rdd1 = sc.parallelize(Array(1,2,3,4,5,6,7,8))
    rdd1.sum()
    //filter
    var rddFilter = sc.parallelize(Array(1,2,3,4,5,6,7,8))
    val res: RDD[Int] = rddFilter.filter(_>3)
    //join
    val rddJoin1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    val rddJoin2 = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7), ("tom", 2)))
    val reeJoinRes: RDD[(String, (Int, Int))] = rddJoin1.join(rddJoin2)
    val leftOuterJoin: RDD[(String, (Int, Option[Int]))] = rddJoin1.leftOuterJoin(rddJoin2)
    val rddrightOuterJoinRes = rddJoin1.rightOuterJoin(rddJoin2)
    // sortBy
//    val sortByRes1: (Int => Nothing) => RDD[Int] = rddFilter.sortBy(x=>x)
    val sortByRes2: RDD[(String, Int)] = rddJoin1.sortBy(_._1)
    sortByRes2.collect()
    //union 没有去重功能
    val rddUnion: RDD[(String, Int)] = rddJoin1.union(rddJoin2)
    // distinct k-v 均相同才去重
    val rddDistinct: RDD[(String, Int)] = rddJoin1.distinct()
    // intersection k-v均相同
    val rddTersection: RDD[(String, Int)] = rddJoin1.intersection(rddJoin2)
    //cogroup 两个RDD的操作
    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = rddJoin1.cogroup(rddJoin2)
    //reduce
    rddJoin2.reduce((a,b)=>{(a._1,a._2+b._2)})
    //takeOrdered
    val takeOrderRes: Array[(String, Int)] = rddJoin1.takeOrdered(2)
    //top
    val topRes: Array[(String, Int)] = rddJoin1.top(2)

    //mapPartitionWithIndex 获取element和 分区index
    var function=(index:Int,it:Iterator[Int])=>{it.map(x=>s"partiotion:$index element:$x")}
    var rddList=sc.parallelize(Array(1,2,3,5,7,9));
    rddList.mapPartitionsWithIndex(function)
    //aggregate 先分区再全局 都会应用 zeroValue action
    rddList.aggregate(0)(_+_,_+_)
    //aggregateByKey
    val aggregateByKeySource = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    aggregateByKeySource.aggregateByKey(0)(_+_,_+_)
    //collectAsMap 以KV形式收集结果
    val collectSource = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    collectSource.collectAsMap() //Map(tom -> 1, kitty -> 3, jerry -> 2)
    collectSource.collect() // Array[(String, Int)] = Array((tom,1), (jerry,2), (kitty,3))
    // foldByKey 和 aggreateByKey 不同于 在局部聚合
    val foldByKeySource = sc.parallelize(List(("tom", "1"), ("tom", "2"), ("kitty", "3")))
    foldByKeySource.foldByKey("|")(_+_)
    foldByKeySource.aggregateByKey("")(_+_,_+_)
    //foreach action 和 foreachPartion
    var foreachSource=sc.parallelize(Array(1,2,3,5,7,9),3)
    val count: LongAccumulator = sc.longAccumulator("vvv")
    foreachSource.foreach(par=>count.add(1))
    foreachSource.foreachPartition(par=>count.add(1)) //结果为2
    // map 和 mapPartition
    foreachSource.map(par=>par+1)
    foreachSource.mapPartitions(x=>x.map(x=>x+1)) //用于分批处理 插入数据库表
    // combineByKey 参数一转换Value类型
    val keysRDD: RDD[Int] = sc.parallelize(Array(1,2,2,2,1))
    val valuesRDD: RDD[String] = sc.parallelize(Array("zs","ls","ww","zl","lq"))
    val combinerSource: RDD[(Int, String)] = keysRDD.zip(valuesRDD)
    val combinerSource1: RDD[(String,Int )] = valuesRDD.zip(keysRDD)

    combinerSource1.combineByKey(x=>x,(m:Int,n:Int)=>m+n,(m:Int,n:Int)=>m+n)

    combinerSource.combineByKey(x=>x.toString,(m:String,n:String)=>m+n,(m:String,n:String)=>m+n)
    combinerSource.combineByKey(x=>ListBuffer(x),(n:ListBuffer[String],m:String)=>n+=m,(n:ListBuffer[String],m:ListBuffer[String])=>n++=m)
  }
}
