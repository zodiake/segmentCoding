package com.ml

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 9/8/2016.
  */
object Gradient {

  def parseInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case _ => 0
    }
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val confCategory = sc.textFile("hdfs://hkgrherfpp016:9000/CATEGORY_CODED_DATA/TMTB/20161409/TMTB.catcoded_1").map(_.split(",")).filter(i => {
      val j = i(1).substring(8, 13)
      j == "10021" || j == "10022" || j == "10020"
    }).filter(_ (0) == "CONF").map(i => (i(1), i(4)))
    val sales = sc.textFile("d:/wangqi/merge").map(_.split(",")).map(i => (i(6), i(3).toFloat))

    confCategory.join(sales).sortBy(i => -i._2._2).take(120).foreach(println)

  }
}
