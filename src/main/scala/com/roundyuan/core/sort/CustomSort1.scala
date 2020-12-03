package com.roundyuan.core.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("sort1").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.parallelize(Array("zhangsan 99 28","lisi 87 28","wangwu 99 26","zhaoliu 88 23"))

    val users:RDD[User]= lines.map(line=>{
      val properties: Array[String] = line.split(" ")
      val name: String = properties(0)
      val pv: Int = properties(1).toInt
      val age: Int = properties(2).toInt
      new User(name,pv,age)
    })
    val sorted: RDD[User] = users.sortBy(u=>u)
    val usersList: Array[User] = sorted.collect()
    usersList.foreach(println)
  }
}
class User(val name:String,val fv:Int,val age:Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {
    if (this.fv==that.fv){
      this.age-that.age
    }else{
      -(this.fv-that.fv)
    }
  }

  override def toString: String = {
    s"$name,$fv,$age"
  }
}