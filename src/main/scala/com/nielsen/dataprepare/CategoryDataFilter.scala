package com.nielsen.dataprepare

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.nielsen.coding.codingUtil

/**
 * @author daiyu01
 */
object CategoryDataFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("CategoryDataFilter")
    val sc = new SparkContext(conf)
    
    val cateRawPathLst = sc.textFile(args(0)).filter(_!="").distinct().collect().toList
    val cateCodeLst = sc.textFile(args(1)).filter(_!="").collect().toList
    
    val templist = Array[String]()
    var ree = sc.parallelize(templist)
    val codUtil = new codingUtil
    for(cateRawPath<-cateRawPathLst){
      val catRawFilter = sc.textFile(cateRawPath).filter { x => cateCodeLst.contains(x.split(",")(0)) }
      ree = catRawFilter ++ ree
    }
    
    
    codUtil.deleteExistPath(args(2))
    ree.saveAsTextFile(args(2))
    
  }
}