package com.nielsen.category

import com.nielsen.coding.codingUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * @author daiyu01
  */
object CateCoUpdate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CateCoUpdate")
    val sc = new SparkContext(conf)

    val fixFile = sc.textFile(args(0)).map(_.split(",")).collect()
    val fixMap = Map.empty[String, String]
    for (fixpair <- fixFile) {
      val id = fixpair(0)
      val catecode = fixpair(1)
      fixMap += (id -> catecode)
    }
    val idLst = fixFile.map(_ (0)).toList
    val finalCate = sc.textFile(args(1)).map { cate =>
      val cateArr = cate.split(",", -1)
      if (idLst.contains(cateArr(1))) {
        cateArr(0) = fixMap(cateArr(1))
      }
      cateArr.mkString(",")
    }

    val codUtil = new codingUtil
    codUtil.deleteExistPath(args(1).concat("_FINAL"))
    finalCate.saveAsTextFile(args(1).concat("_FINAL"))

  }
}