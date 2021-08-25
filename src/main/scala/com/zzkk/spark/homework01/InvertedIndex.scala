package com.zzkk.spark.homework01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("InvertedIndex").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val oriRDD: RDD[(String, String)] = sc.wholeTextFiles("data\\")

    val wcRDD: RDD[(String, Int)] = oriRDD.flatMap(x => {
      //获取文件名
      val fileName: String = x._1.split(s"/").last.split("\\.").head
      //切分每个字段，并组合起来求 wordCount
      x._2.split("\r\n")
        .flatMap(line => line.split(" "))
        .map(word => (word + "-" + fileName, 1))
    }).reduceByKey(_ + _)

    //调整RDD结构（word,(filename, count)）
    val resultRDD: RDD[(String, Iterable[(String, Int)])] = wcRDD.map(x => (x._1.split("-").head, (x._1.split("-").last, x._2)))
      .groupByKey()

    //格式化输出
    resultRDD.collect.foreach(x => println(s"${x._1}: {${x._2.toList.mkString(",")}}"))

    sc.stop()
  }
}
