package com.spark.coding

import com.esotericsoftware.kryo.KryoSerializable
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 28/7/2016.
  */
object SparkMain {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf().setMaster("local[2]").setAppName("BrandCoing")

    val sc = new SparkContext(conf)
    val src = sc.textFile("d:/wangqi/hh")

    src.take(10).foreach(println)
  }
}
