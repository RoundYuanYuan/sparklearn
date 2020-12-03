package com.roundyuan.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.{JsValue, Json}

object ReadJsonBySparkCore extends App {
    val conf = new SparkConf().setAppName("readJson").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.parallelize(List("""{"name":"过往记忆","website":"www.iteblog.com"}""", """{"name":"过往记忆"}"""))
    val jsons: RDD[JsValue] = lines.map(Json.parse)
    val parseReads = Json.format[Info]
    val records = jsons.flatMap(record=>parseReads.reads(record).asOpt)
    records.foreach(println)
}
