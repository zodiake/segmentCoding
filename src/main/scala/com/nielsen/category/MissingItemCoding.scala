package com.nielsen.category

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import com.nielsen.coding.codingUtil

object MissingItemCoding {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
  //  conf.setMaster("local")
    conf.setAppName("MissingItemCoding")
    val sc = new SparkContext(conf)
    
   // val newItem = sc.textFile("C://Users//Mirra//Desktop//NewItem")
    val newItem = sc.textFile(args(0))
    val newItemId = newItem.map(_.split(",")(15)).collect().toList
    //val category = sc.textFile("C://Users//Mirra//Desktop//Category").map(_.split(",")(1)).collect().toList
    val category = sc.textFile(args(1)).map(_.split(",")(1)).collect().toList
    val missingItemId = sc.parallelize((newItemId:::category).map { x => (x,1) })
   // println(missingItemId.collect().toList)
    val missingItemIds = new PairRDDFunctions(missingItemId).reduceByKey(_+_).collect().filter(_._2 == 1).map(_._1).toList
    
    val missingItem = newItem.filter { x =>missingItemIds.contains(x.split(",")(15)) }
    
    val util = new codingUtil
    util.deleteExistPath(args(0)+"_MissingItem")
    missingItem.saveAsTextFile(args(0)+"_MissingItem")
    
  }
}