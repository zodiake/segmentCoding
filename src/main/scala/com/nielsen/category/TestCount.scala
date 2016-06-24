package com.nielsen.category

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import com.nielsen.coding.codingUtil

object TestCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
  //  conf.setMaster("local")
    conf.setAppName("MissingItemCodingTest")
    val sc = new SparkContext(conf)
    
   // val newItem = sc.textFile("C://Users//Mirra//Desktop//NewItem")
    val categoryItem = sc.textFile(args(1))
    val newItemId = sc.textFile(args(0)).map(_.split(",")(0))
    //val category = sc.textFile("C://Users//Mirra//Desktop//Category").map(_.split(",")(1)).collect().toList
    val category = categoryItem.map(_.split(",")(1))
    val missingItemId = (newItemId++category).map { x => (x,1) }
   // println(missingItemId.collect().toList)
    val missingItemIds = new PairRDDFunctions(missingItemId).reduceByKey(_+_).filter(_._2 == 1).map(_._1).collect().toList
    
    val missingItem = categoryItem.filter { x =>missingItemIds.contains(x.split(",")(1)) }
    
    val util = new codingUtil
    util.deleteExistPath(args(0)+"_MissingItem")
    missingItem.saveAsTextFile(args(0)+"_MissingItem")
    
  }
}