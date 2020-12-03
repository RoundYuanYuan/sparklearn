package com.roundyuan.core

import scala.io.{BufferedSource, Source}

object AccessCount {
  def main(args: Array[String]): Unit = {
    //数据是在内存中
    val rules: Array[(Long, Long, String)] = readRules("F:\\5-File\\7-code\\SparkLearn\\src\\main\\resources\\ip.txt")
    //将ip地址转换成十进制
    val ipNum = ip2long("114.215.43.42")
    //查找
    val index = binarySearchProvince(rules, ipNum)
    //根据脚本到rules中查找对应的数据
    val tp = rules(index)
    val province = tp._3
    println(province)

  }

  //将IP地址转换为10进制
  def ip2long(ip: String): Long = {
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- fragments) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  //二分查找
  def binarySearch(ipLong: Long, ipArray: Array[Long]): Int = {
    var index = -1
    var start = 0;
    var end = ipArray.length - 1
    while (start <= end) {
      val mid = (start + end + 1) / 2
      if (ipLong == ipArray(mid)) {
        index = mid
        return index
      } else if (ipLong < ipArray(mid)) {
        end = mid - 1
      } else {
        start = mid + 1
      }
    }
    index
  }
  def binarySearchProvince(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }
  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }
}
