package com.spark.coding

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 28/7/2016.
  */
object SparkMain {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf().setMaster("local[2]").setAppName("BrandCoing")

    val sc = new SparkContext(conf)
    val src = sc.textFile("hdfs://10.250.33.92:9000/RAW_DATA/TMTB/20161406/TMTB.csv_1")

    val descFormat = src.map { x => (x, x.split(",", -1)) }.filter(_._2.length >= 20).filter(i => i._2(12).contains("天猫超市"))
      .map { x => (x._1, x._2(4) + "," + x._2(5) + "," + x._2(6), x._1.indexOf(x._2(4) + "," + x._2(5) + "," + x._2(6))) }
      .map { x => (x._1.substring(0, x._3), "2", x._1.substring(x._3 + x._2.length())) }
      .map(x => x._1 + x._2 + x._3)

    val res = descFormat.map(row => {
      val array = row.split(",")
      if (array(3) == "B" || array(3) == "C")
        if (array(10).indexOf("天猫超市") == 2)
          array(3) = "S"
      array.mkString(",")
    })

    res.take(10).foreach(println)
  }
}
