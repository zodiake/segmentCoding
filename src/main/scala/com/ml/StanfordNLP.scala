package com.ml

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 10/8/2016.
  */
object StanfordNLP {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)
    // 载入自定义的Properties文件
  }
}
