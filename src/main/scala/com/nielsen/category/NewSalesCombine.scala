package com.nielsen.category

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.nielsen.coding.codingUtil
import org.apache.spark.rdd.PairRDDFunctions

/**
 * @author daiyu01
 */
object NewSalesCombine {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("NewSalesCombine")
    val sc = new SparkContext(conf)
    val salesExceptTM = sc.textFile(args(0)).map(_.split(",")).filter { sales => sales(8) != "10010" }.map { x => (x(6),x(3).toDouble) }
    val salesExceptTMPairRdd = new PairRDDFunctions(salesExceptTM)
    val salesCombine = salesExceptTMPairRdd.reduceByKey(_+_).map(x=> x._1+"," + x._2)
    val codingUtil = new codingUtil
    codingUtil.deleteExistPath(args(0).concat("_COMBINE"))
    salesCombine.saveAsTextFile(args(0).concat("_COMBINE"))
  }
}