package com.roundyuan.core

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRDDDemo {
  val getConnection=()=>{
    DriverManager.getConnection("jdbc:mysql://192.168.88.21:3306/final_design?characterEncoding=UTF-8", "root", "123456")
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sql 时用全包含>= 和 <= 在分区读取时 会 切分区间保留sql
    val sql="select * from t_city where id >=? and id < ?"
    val jdbcRDD = new JdbcRDD(sc,getConnection,sql,2,7,2,
      rs=>{
        val id=rs.getInt(1)
        val name=rs.getString(2)
        val parID=rs.getInt(3)
        (id,name,parID)
      })
    val res = jdbcRDD.collect()
    res.foreach(println)
  }
}
