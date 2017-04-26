package com.nielsen.category

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.nielsen.coding.codingUtil

/**
  * @author daiyu01
  */
object CateResultFormat {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("CategoryResultFormat")
    val sc = new SparkContext(conf)
    val result = sc.textFile(args(0))
      .map { x => x.split(",", -1) }
      .map { x => x(1) + "," + x(0) + "," + x(1).substring(0, 8) + "," + x(1).substring(8, 13) }
    val codutil = new codingUtil
    codutil.deleteExistPath(args(0) + "_format")
    result.saveAsTextFile(args(0) + "_format")
  }
}