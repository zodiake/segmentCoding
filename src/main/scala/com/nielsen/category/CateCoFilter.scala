package com.nielsen.category

import com.nielsen.coding.codingUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.PairRDDFunctions

/**
  * @author daiyu01
  */
/*
    arg0:source data
    arg1:sales data
    arg2:segconfig
    arg3:top
 */
object CateCoFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CateCoFilter")
    val sc = new SparkContext(conf)
    val num = args(3).toInt
    val cateCoLstTmp = sc.textFile(args(2)).map { x => x.split(",")(1) }.distinct().collect().toList
    val cateCoLst = "NC" :: cateCoLstTmp
    val jd: Array[String] => Boolean = i => i(1).substring(8, 13) == "20125" || i(1).substring(8, 13) == "20127"
    val other: Array[String] => Boolean = i => i(1).substring(8, 13) != "10010"
    val filtered = if (args(4) == "JDFULL") jd else other
    val cateCodingText = sc.textFile(args(0)).map(_.split(",")).filter(x => cateCoLst.contains(x(0)))
      .filter(filtered).map { cate => (cate(1), (cate(0), cate(2), cate(4))) }
    val salesText = sc.textFile(args(1)).map(_.split(",")).map { x => (x(0), x(1)) }
    val cateCoPairRdd = new PairRDDFunctions(cateCodingText)
    val combine = cateCoPairRdd.leftOuterJoin(salesText).filter(!_._2._2.isEmpty).map { x =>
      var cateCode = x._2._1._1
      var itemid = x._1
      var brandType = x._2._1._2
      var desc = x._2._1._3
      var salesValue = x._2._2.get.toDouble
      var t = "tsi"
      val store = itemid.substring(8, 13)
      val period = itemid.substring(0, 8)
      (cateCode, (salesValue, (cateCode, itemid, brandType, desc, t, store, period)))
    }
    val combinePairRdd = new PairRDDFunctions(combine).groupByKey().map { x => (x._1, x._2.toList.sortBy(_._1).takeRight(num)) }.map(_._2).reduce(_ ::: _)
      .map(x => x._2._1 + "," + x._2._5 + "," + x._2._6 + "," + x._2._7 + "," + x._2._2 + "," + x._2._3 + "," + x._2._4 + "," + x._1)
    val codingUtil = new codingUtil
    codingUtil.deleteExistPath(args(0).concat("_FILTER_").concat(num.toString()))
    val rdd = sc.parallelize(combinePairRdd)
    rdd.saveAsTextFile(args(0).concat("_FILTER_").concat(num.toString()))
  }
}