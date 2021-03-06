package com.nielsen.dataformat

import com.nielsen.coding.codingUtil
import org.apache.spark.{SparkConf, SparkContext}

/*
 * update category level1 for TMTB
 * arg(0):hdfs://hkgrherfpp016:9000/RAW_DATA/TMTB/20161412/TMTB.csv_1
 * arg(1):hdfs://hkgrherfpp016:9000/CONF_DATA/ModelMapping
 */
object TMTBFormat {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //conf.setAppName("TMTBFormat")
    //conf.setMaster("local")

    val sc = new SparkContext(conf)

    val desc = sc.textFile(args(0))
    val config = sc.textFile(args(1))

    val configMap = config.map(_.split(" ")).map(x => (x(0), x(1))).collect().toMap

    val descFormat = desc.map { x => (x, x.split(",", -1)) }.filter(_._2.length >= 20)
      .map { x => (x._1, x._2(4) + "," + x._2(5) + "," + x._2(6), x._1.indexOf(x._2(4) + "," + x._2(5) + "," + x._2(6))) }
      .map { x => (x._1.substring(0, x._3), formatFunc(x._2, configMap), x._1.substring(x._3 + x._2.length())) }
      .map(x => x._1 + x._2 + x._3)
      .map { i =>
        val array = i.split(",")
        array(9) = array(9).replaceAll("天猫", "TMALL")
        array(10) = array(10).replaceAll("天猫", "TMALL")
        array(12) = array(12).replaceAll("天猫", "TMALL")
        array(19) = array(19).replaceAll("天猫", "TMALL")
        array.mkString(",")
      }

    val filter = desc.filter { x => x.split(",", -1).length < 20 }
    val util = new codingUtil

    util.deleteExistPath(args(0) + ".FORMAT")
    util.deleteExistPath(args(0) + ".FILTER")

    val res = descFormat.map(row => {
      val array = row.split(",")
      if (array(3) == "B" || array(3) == "C")
        if (array(12).indexOf("天猫超市") == 2 || array(0).indexOf("天猫超市") > -1)
          array(3) = "S"
      array.mkString(",")
    })

    res.saveAsTextFile(args(0) + ".FORMAT")
    filter.saveAsTextFile(args(0) + ".FILTER")
  }

  def formatFunc(catelv123: String, configMap: Map[String, String]): String = {
    val strlst = catelv123.split(",", -1)
    var str = catelv123
    if (configMap.contains(catelv123)) {
      str = configMap.get(catelv123).get + "," + strlst(1) + "," + strlst(2)
    }
    str
  }
}