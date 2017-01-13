package com.nielsen.dataprepare

import java.net.URI

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by zodiake on 17-1-13.
  */
object JDSplit {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //conf.setAppName("asdf")
    //conf.setMaster("local")
    val sc = new SparkContext(conf)

    //添加批次的改动
    var batchNum = args(3)
    val source = args(0)
    val itemTarget = args(1)
    val salesTarget = args(2)

    val file = sc.textFile(source).map(_.split(",\"")).filter(_.size == 6)
      .filter(x => x(4).split("\",").size == 2)
      .map(x => raw_split(x)).filter(!_.isEmpty).map(_.toList)

    val fileGrouped = file.map(row => (getKey(row), row)).groupByKey().sortByKey().zipWithIndex

    val map = sc.broadcast(Map(
      "20127" -> "非全球购",
      "20157" -> "全球购",
      "20126" -> "非全球购",
      "20124" -> "全球购"
    ))

    val items = fileGrouped map {
      case ((key, salesItems), idSeed) =>
        val r = salesItems.map(_ (18).toFloat).sum / salesItems.size
        val keyArray = key.split(",")
        s"${key},${r},${keyArray(0)}${keyArray(4)}${"%09.0f".format(idSeed * 1.0 + 1) + batchNum},${map.value(keyArray(4))},"
    }

    val sales = fileGrouped flatMap {
      case ((key, salesItems), idSeed) =>
        salesItems.map { row =>
          val arrayKey = key.split(",")
          val suffix = s"${arrayKey.head}${arrayKey(4)}${"%09.0f".format(idSeed * 1.0 + 1) + batchNum}"
          val prefix = s"${row(14).trim()},${row(15).trim()},${row(16).trim()},${row(17).trim()},${row(18).trim()},${row(19)} 00:00:00"
          val city = row(2)
          s"${prefix},${suffix},${suffix.substring(0, 8)},${suffix.substring(8, 13)},${city}"
        }
    }


    def deleteExistPath(pathRaw: String) {
      val outPut = new Path(pathRaw)
      val hdfs = FileSystem.get(URI.create(pathRaw), new Configuration())
      if (hdfs.exists(outPut)) hdfs.delete(outPut, true)
    }

    deleteExistPath(itemTarget)
    deleteExistPath(salesTarget)
    items.saveAsTextFile(itemTarget)
    sales.saveAsTextFile(salesTarget)
  }

  def raw_split(raw_data: Array[String]): Array[String] = {
    val fi0 = raw_data(0) + "," + raw_data(1).split("\",").head.replace(",", "") + "," + raw_data(1).split("\",").reverse.head + "," + raw_data(2).replace("\"", "") + "," + raw_data(3).replace("\"", "")

    val fi4 = raw_data(4).split("\",")

    val fi4_1 = fi4(0).replace(",", "")

    val fi4_2 = fi4(1)

    val fi5 = raw_data(5).
      replace("\",", "*%$#").replace(",", "").replace("*%$#", ",")

    val fi = (fi0 + "," + fi4_1 + "," + fi4_2 + "," + fi5).split(",", -1)

    if (fi.size == 21 || fi.size == 20) {
      if (fi(18).size == 10) {
        return fi(18).substring(0, 7).replace("-", "14") +: fi
      } else {
        return Array[String]()
      }
    } else {
      return Array[String]()
    }
  }

  def getKey(list: List[String]): String = {
    List(list(0), "", "", list(3), list(4), list(5), list(6), list(7), list(8), list(9), list(10), list(11), list(13), list(20)).mkString(",")
  }
}
