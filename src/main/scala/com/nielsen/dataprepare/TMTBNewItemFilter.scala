package com.nielsen.dataprepare

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.sun.beans.decoder.TrueElementHandler
import com.nielsen.coding.codingUtil

/**
 * @author daiyu01
 */


/**
 * @author daiyu01
 */
object TMTBNewItemFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("TMTBNewItemFilter")
    val sc = new SparkContext(conf)
    
    val filterFile = sc.textFile(args(0)).map { x => x.split(",") }.collect().toList
    val itemFile = sc.textFile(args(1)).map { x => x.split(",") }.filter { x => needToBeRm(x(5),x(6),filterFile) }.map { x => x.mkString(",") }
    val codUtil = new codingUtil
    codUtil.deleteExistPath(args(1).concat("_Filter"))
    itemFile.saveAsTextFile(args(1).concat("_Filter"))
  }
  
  def needToBeRm(cateLv1:String,cateLv2:String,filterlst:List[Array[String]]):Boolean={
    var flag = true 
    for(filter <- filterlst){
      if(cateLv1 == filter(0) && cateLv2 == filter(1)){
        flag = false
      }
    }
    return flag
  }
}